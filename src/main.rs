extern crate byteorder;
extern crate clap;
extern crate crossbeam_channel;
extern crate flate2;
extern crate geo;
#[macro_use]
extern crate itertools;
#[macro_use]
extern crate log;
extern crate postgres;
extern crate postgres_binary_copy;
extern crate pretty_env_logger;
extern crate streaming_iterator;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::iter::Iterator;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;

use byteorder::{NetworkEndian, ReadBytesExt};
use clap::{App, Arg};
use crossbeam_channel::bounded;
use flate2::read::ZlibDecoder;
use geo::Point;
use itertools::Itertools;
use postgres::{Connection, TlsMode};
use postgres::types::{INT8, ToSql, Type};
use postgres_binary_copy::BinaryCopyReader;
use streaming_iterator::StreamingIterator;

mod protos;
use protos::fileformat::{BlobHeader, Blob};
use protos::osmformat::PrimitiveBlock;

fn main() {
    pretty_env_logger::init_timed();

    let args = App::new("osm2pg")
        .version("0.1")
        .about("Rapidly imports an OSM .pbf file into a PostgreSQL database")
        .author("jbg")
        .arg(Arg::with_name("db")
            .short("d")
            .long("db")
            .value_name("URL")
            .help("the PostgreSQL connection URL string specifying the server to connect to (can also be supplied as OSM2PG_DATABASE_URL environment variable)")
            .takes_value(true))
        .arg(Arg::with_name("INPUT")
             .help("the input file in OSM .pbf format")
             .required(true)
             .index(1))
        .get_matches();

    let db_url = args.value_of("db")
        .expect("please supply the database URL using the -d parameter or OSM2PG_DATABASE_URL environment variable")
        .to_owned();

    let mut handles = Vec::new();

    {
        let node_sender = {
            let (node_sender, node_receiver) = bounded(2000000);

            {
                let db = Connection::connect(db_url.clone(), TlsMode::None).unwrap();
                db.execute("CREATE UNLOGGED TABLE node (id BIGINT NOT NULL, coordinates POINT NOT NULL, tags HSTORE NOT NULL)", &[]).unwrap();
                db.execute("CREATE UNLOGGED TABLE way (id BIGINT NOT NULL, tags HSTORE NOT NULL)", &[]).unwrap();
                db.execute("CREATE UNLOGGED TABLE relation (id BIGINT NOT NULL, tags HSTORE NOT NULL)", &[]).unwrap();
            }

            for i in 1..3 {
                let local_node_receiver = node_receiver.clone();
                let node_db_url = db_url.clone();
                handles.push(thread::Builder::new()
                    .name(format!("node-writer-{}", i))
                    .spawn(move || {
                        info!("node-writer {} connecting to DB...", i);
                        let db = Connection::connect(node_db_url, TlsMode::None).unwrap();
                        let stmt = db.prepare("SELECT $1::point, $2::hstore").unwrap();
                        let types = stmt.param_types();
                        let point_type = types[0].clone();
                        let hstore_type = types[1].clone();
                        copy_in(
                            &db,
                            &[INT8, point_type, hstore_type],
                            "COPY node (id, coordinates, tags) FROM STDIN (FORMAT binary)",
                            local_node_receiver.iter()
                        );
                        info!("node-writer {} finished", i);
                    })
                    .unwrap());
            }

            node_sender
        };

        let way_sender = {
            let (way_sender, way_receiver) = bounded(200000);
            let way_db_url = db_url.clone();
            handles.push(thread::Builder::new()
                .name("way-writer".into())
                .spawn(move || {
                    info!("way-writer connecting to DB...");
                    let db = Connection::connect(way_db_url, TlsMode::None).unwrap();
                    let stmt = db.prepare("SELECT $1::hstore").unwrap();
                    let hstore_type = stmt.param_types()[0].clone();
                    copy_in(
                        &db,
                        &[INT8, hstore_type],
                        "COPY way (id, tags) FROM STDIN (FORMAT binary)",
                        way_receiver.iter()
                    );
                    info!("way-writer finished");
                })
                .unwrap());
            way_sender
        };

        let relation_sender = {
            let (relation_sender, relation_receiver) = bounded(20000);
            let relation_db_url = db_url.clone();
            handles.push(thread::Builder::new()
                .name("relation-writer".into())
                .spawn(move || {
                    info!("relation-writer connecting to DB...");
                    let db = Connection::connect(relation_db_url, TlsMode::None).unwrap();
                    let stmt = db.prepare("SELECT $1::hstore").unwrap();
                    let hstore_type = stmt.param_types()[0].clone();
                    copy_in(
                        &db,
                        &[INT8, hstore_type],
                        "COPY relation (id, tags) FROM STDIN (FORMAT binary)",
                        relation_receiver.iter()
                    );
                    info!("relation-writer finished");
                })
                .unwrap());
            relation_sender
        };

        let block_sender = {
            let (block_sender, block_receiver) = bounded::<PrimitiveBlock>(64);
            for i in 1..3 {
                let local_block_receiver = block_receiver.clone();
                let local_node_sender = node_sender.clone();
                let local_way_sender = way_sender.clone();
                let local_relation_sender = relation_sender.clone();
                handles.push(thread::Builder::new()
                    .name(format!("block-parser-{}", i))
                    .spawn(move || {
                        while let Ok(block) = local_block_receiver.recv() {
                            let strings = block.get_stringtable().get_s().to_vec();
                            let groups = block.get_primitivegroup();
                            let granularity = block.get_granularity();
                            let lat_offset = block.get_lat_offset();
                            let lon_offset = block.get_lon_offset();

                            for group in groups.into_iter() {
                                // All OSM dumps I've come across have dense nodes
                                assert!(group.get_nodes().is_empty());

                                let dense = group.get_dense();
                                let node_ids = decode_osm_deltas(dense.get_id().into_iter());
                                let node_lats = decode_osm_deltas(dense.get_lat().into_iter())
                                    .map(|lat| coord(lat, lat_offset, granularity));
                                let node_lons = decode_osm_deltas(dense.get_lon().into_iter())
                                    .map(|lon| coord(lon, lon_offset, granularity));

                                let node_coords = izip!(node_lons, node_lats).map(Point::from);

                                let node_tags = dense.get_keys_vals().into_iter()
                                    .batching(|iter| Some(
                                        iter.take_while(|i| **i != 0)
                                            .map(|i| String::from_utf8_lossy(&strings[*i as usize]).into_owned())
                                            .tuples::<(_, _)>()
                                            .map(|(k, v)| (k, Some(v)))
                                            .collect::<HashMap<_, _>>()
                                    ));
                               
                                for (id, coords, tags) in izip!(node_ids, node_coords, node_tags) {
                                    let row: Vec<Box<ToSql + Send>> = vec![Box::new(id), Box::new(coords), Box::new(tags)];
                                    local_node_sender.send(row).unwrap();
                                }

                                for way in group.get_ways().into_iter() {
                                    let tags = izip!(
                                        way.get_keys().into_iter()
                                            .map(|i| String::from_utf8_lossy(&strings[*i as usize]).into_owned()),
                                        way.get_vals().into_iter()
                                            .map(|i| Some(String::from_utf8_lossy(&strings[*i as usize]).into_owned())),
                                    ).collect::<HashMap<_, _>>();
                                     
                                    let row: Vec<Box<ToSql + Send>> = vec![Box::new(way.get_id()), Box::new(tags)];
                                    local_way_sender.send(row).unwrap();

                                    // TODO refs
                                }

                                for rel in group.get_relations().into_iter() {
                                    let tags = izip!(
                                        rel.get_keys().into_iter()
                                            .map(|i| String::from_utf8_lossy(&strings[*i as usize]).into_owned()),
                                        rel.get_vals().into_iter()
                                            .map(|i| Some(String::from_utf8_lossy(&strings[*i as usize]).into_owned())),
                                    ).collect::<HashMap<_, _>>();
                                    
                                    let row: Vec<Box<ToSql + Send>> = vec![Box::new(rel.get_id()), Box::new(tags)];
                                    local_relation_sender.send(row).unwrap();

                                    // TODO refs
                                    // TODO members
                                }
                            }
                        }
                        info!("finished parsing blocks ({})", thread::current().name().unwrap());
                    })
                    .unwrap());
            }
            block_sender
        };

        let blob_sender = {
            let (blob_sender, blob_receiver) = bounded::<(Vec<u8>, usize)>(64);
            for i in 1..3 {
                let local_blob_receiver = blob_receiver.clone();
                let local_block_sender = block_sender.clone();
                handles.push(thread::Builder::new()
                    .name(format!("blob-decompresser-{}", i))
                    .spawn(move || {
                        while let Ok((compressed_data, decompressed_len)) = local_blob_receiver.recv() {
                            let mut buf = vec![0u8; decompressed_len];
                            ZlibDecoder::new(compressed_data.as_ref() as &[u8])
                                .read_exact(&mut buf)
                                .unwrap();
                            let block: PrimitiveBlock = protobuf::parse_from_bytes(&buf).unwrap();
                            local_block_sender.send(block).unwrap();
                        }
                        info!("finished decompressing blobs ({})", thread::current().name().unwrap());
                    })
                    .unwrap());
            }
            blob_sender
        };

        {
            let input_file = args.value_of("INPUT").unwrap().to_owned();
            info!("starting OSM import from {}", input_file);

            let local_blob_sender = blob_sender.clone();
            handles.push(thread::Builder::new()
                .name("pbf-reader".into())
                .spawn(move || {
                    let mut file = File::open(input_file).unwrap();
                    loop {
                        let (field_type, field_size) = {
                            let header_len = match file.read_i32::<NetworkEndian>() {
                                Ok(len) => len,
                                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                                Err(e) => panic!("{:?}", e)
                            };
                            let mut buf = vec![0u8; header_len as usize];
                            file.read_exact(&mut buf).unwrap();
                            let mut header: BlobHeader = protobuf::parse_from_bytes(&buf).unwrap();
                            (header.take_field_type(), header.get_datasize() as usize)
                        };
                        let mut blob: Blob = {
                            let mut buf = vec![0u8; field_size];
                            file.read_exact(&mut buf).unwrap();
                            protobuf::parse_from_bytes(&buf).unwrap()
                        };

                        match field_type.as_str() {
                            "OSMHeader" => {
                                info!("file contains valid OSM header");
                            },
                            "OSMData" => {
                                // All OSM dumps I've come across have only zlib-compressed data.
                                assert!(blob.has_zlib_data());
                                local_blob_sender.send((blob.take_zlib_data(), blob.get_raw_size() as usize)).unwrap();
                            },
                            _ => panic!("unexpected field type")
                        }
                    }
                    info!("finished reading input file");
                })
                .unwrap());
        }
    }

    for handle in handles.into_iter().rev() {
        handle.join().unwrap();
    }

    let db_url_1 = db_url.clone();
    let db_url_2 = db_url.clone();
    info!("creating indexes...");
    let handles = vec![
        thread::Builder::new()
            .name("node-indexes".into())
            .spawn(move || {
                let db = Connection::connect(db_url, TlsMode::None).unwrap();
                db.execute("ALTER TABLE node SET LOGGED", &[]).unwrap();
                db.execute("ALTER TABLE node ADD PRIMARY KEY (id)", &[]).unwrap();
                db.execute("CREATE INDEX node_coordinates USING gist ON node (coordinates)", &[]).unwrap();
                db.execute("CREATE INDEX node_tags USING gin ON node (tags)", &[]).unwrap();
                info!("node indexes created");
            })
            .unwrap(),
        thread::Builder::new()
            .name("way-indexes".into())
            .spawn(move || {
                let db = Connection::connect(db_url_1, TlsMode::None).unwrap();
                db.execute("ALTER TABLE way SET LOGGED", &[]).unwrap();
                db.execute("ALTER TABLE way ADD PRIMARY KEY (id)", &[]).unwrap();
                db.execute("CREATE INDEX way_tags USING gin ON way (tags)", &[]).unwrap();
                info!("way indexes created");
            })
            .unwrap(),
        thread::Builder::new()
            .name("relation-indexes".into())
            .spawn(move || {
                let db = Connection::connect(db_url_2, TlsMode::None).unwrap();
                db.execute("ALTER TABLE relation SET LOGGED", &[]).unwrap();
                db.execute("ALTER TABLE relation ADD PRIMARY KEY (id)", &[]).unwrap();
                db.execute("CREATE INDEX relation_tags USING gin ON relation (tags)", &[]).unwrap();
                info!("relation indexes created");
            })
            .unwrap()
    ];
    for handle in handles.into_iter().rev() {
        handle.join().unwrap();
    }

    info!("import complete");
}

fn copy_in<I>(db: &Connection, types: &[Type], sql: &'static str, iter: I)
where
    I: Iterator<Item=Vec<Box<ToSql + Send>>>
{
    let values = streaming_iterator::convert(iter.flatten())
        .map_ref(|v| &**v as &ToSql);
    let mut reader = BinaryCopyReader::new(types, values);
    let stmt = db.prepare(sql).unwrap();
    stmt.copy_in(&[], &mut reader).unwrap();
}

fn coord(v: i64, offset: i64, granularity: i32) -> f64 {
    0.000000001 * (offset as f64 + (granularity as f64 * v as f64))
}

fn decode_osm_deltas<'a>(i: impl Iterator<Item=&'a i64> + 'a) -> impl Iterator<Item=i64> + 'a {
    let accumulator = AtomicI64::new(0);
    i.map(move |j| j + accumulator.fetch_add(*j, Ordering::Relaxed))
}
