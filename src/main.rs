extern crate byteorder;
extern crate clap;
extern crate crossbeam_channel;
extern crate flate2;
#[macro_use]
extern crate itertools;
#[macro_use]
extern crate log;
extern crate postgres;
extern crate postgres_binary_copy;
extern crate pretty_env_logger;
extern crate streaming_iterator;

use std::fs::File;
use std::io::Read;
use std::iter::Iterator;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::Duration;

use byteorder::{NetworkEndian, ReadBytesExt};
use clap::{App, Arg};
use crossbeam_channel::{bounded, tick, unbounded};
use flate2::read::ZlibDecoder;
use postgres::{Connection, TlsMode};
use postgres::types::{FLOAT8, INT8, ToSql, Type};
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
        info!("allocating data structures...");

        let node_counter = Arc::new(AtomicI64::new(0));
        let (node_sender, node_receiver) = bounded(200000000);

        {
            let db = Connection::connect(db_url.clone(), TlsMode::None).unwrap();
            db.execute("CREATE UNLOGGED TABLE node (id BIGINT NOT NULL, lat DOUBLE PRECISION NOT NULL, lon DOUBLE PRECISION NOT NULL)", &[]).unwrap();
            db.execute("CREATE UNLOGGED TABLE way (id BIGINT NOT NULL)", &[]).unwrap();
            db.execute("CREATE UNLOGGED TABLE relation (id BIGINT NOT NULL)", &[]).unwrap();
        }

        for i in 1..7 {
            let local_node_receiver = node_receiver.clone();
            let local_node_counter = node_counter.clone();
            let node_db_url = db_url.clone();
            handles.push(thread::Builder::new()
                .name(format!("node-writer-{}", i))
                .spawn(move || {
                    info!("node-writer {} connecting to DB...", i);
                    let db = Connection::connect(node_db_url, TlsMode::None).unwrap();
                    copy_in(
                        &db,
                        &[INT8, FLOAT8, FLOAT8],
                        "COPY node (id, lat, lon) FROM STDIN (FORMAT binary)",
                        local_node_receiver.iter()
                            .inspect(|_| { local_node_counter.fetch_add(1, Ordering::Relaxed); })
                    );
                    info!("node-writer {} finished", i);
                })
                .unwrap());
        }

        let way_counter = Arc::new(AtomicI64::new(0));
        let local_way_counter = way_counter.clone();
        let (way_sender, way_receiver) = unbounded();
        let way_db_url = db_url.clone();
        handles.push(thread::Builder::new()
            .name("way-writer".into())
            .spawn(move || {
                info!("way-writer connecting to DB...");
                let db = Connection::connect(way_db_url, TlsMode::None).unwrap();
                copy_in(
                    &db,
                    &[INT8],
                    "COPY way (id) FROM STDIN (FORMAT binary)",
                    way_receiver.iter()
                        .inspect(|_| { local_way_counter.fetch_add(1, Ordering::Relaxed); })
                );
                info!("way-writer finished");
            })
            .unwrap());

        let relation_counter = Arc::new(AtomicI64::new(0));
        let local_relation_counter = relation_counter.clone();
        let (relation_sender, relation_receiver) = unbounded();
        let relation_db_url = db_url.clone();
        handles.push(thread::Builder::new()
            .name("relation-writer".into())
            .spawn(move || {
                info!("relation-writer connecting to DB...");
                let db = Connection::connect(relation_db_url, TlsMode::None).unwrap();
                copy_in(
                    &db,
                    &[INT8],
                    "COPY relation (id) FROM STDIN (FORMAT binary)",
                    relation_receiver.iter()
                        .inspect(|_| { local_relation_counter.fetch_add(1, Ordering::Relaxed); })
                );
                info!("relation-writer finished");
            })
            .unwrap());

        let block_counter = Arc::new(AtomicI64::new(0));
        let (block_sender, block_receiver) = bounded::<PrimitiveBlock>(256);
        for i in 1..9 {
            let local_block_receiver = block_receiver.clone();
            let local_node_sender = node_sender.clone();
            let local_way_sender = way_sender.clone();
            let local_relation_sender = relation_sender.clone();
            let local_block_counter = block_counter.clone();
            handles.push(thread::Builder::new()
                .name(format!("block-parser-{}", i))
                .spawn(move || {
                    while let Ok(block) = local_block_receiver.recv() {
                        // let strings = block.take_stringtable().take_s().into_vec();
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

                            // TODO https://github.com/sfackler/rust-postgres-binary-copy/issues/4
                            //      after resolution, insert these tags (plus the way and relation tags
                            //      below) as hstore
                            //
                            // let node_kvs = dense.take_keys_vals().into_iter()
                            //     .batching(|iter| Some(
                            //         iter.take_while(|i| *i != 0)
                            //             .map(|i| String::from_utf8_lossy(&strings[i as usize]).into_owned())
                            //             .tuples::<(_, _)>()
                            //             .map(|(k, v)| (k, Some(v)))
                            //             .collect::<HashMap<_, _>>()
                            //     ));
                            //
                            // TODO insert lat/lon as a geometry POINT field

                            for (id, lat, lon) in izip!(node_ids, node_lats, node_lons) {
                                let row: Vec<Box<ToSql + Send>> = vec![Box::new(id), Box::new(lat), Box::new(lon)];
                                local_node_sender.send(row).unwrap();
                            }

                            for way in group.get_ways().into_iter() {
                                // let kv: HashMap<String, Option<String>> = izip!(
                                //     way.take_keys().into_iter()
                                //         .map(|i| String::from_utf8_lossy(&strings[i as usize]).into_owned()),
                                //     way.take_vals().into_iter()
                                //         .map(|i| Some(String::from_utf8_lossy(&strings[i as usize]).into_owned())),
                                // ).collect();
                                 
                                let row: Vec<Box<ToSql + Send>> = vec![Box::new(way.get_id())];
                                local_way_sender.send(row).unwrap();

                                // TODO refs
                            }

                            for rel in group.get_relations().into_iter() {
                                // let kv: HashMap<String, Option<String>> = izip!(
                                //     rel.take_keys().into_iter()
                                //         .map(|i| String::from_utf8_lossy(&strings[i as usize]).into_owned()),
                                //     rel.take_vals().into_iter()
                                //         .map(|i| Some(String::from_utf8_lossy(&strings[i as usize]).into_owned())),
                                // ).collect();
                                
                                let row: Vec<Box<ToSql + Send>> = vec![Box::new(rel.get_id())];
                                local_relation_sender.send(row).unwrap();

                                // TODO refs
                                // TODO members
                            }

                            local_block_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    info!("finished parsing blocks ({})", thread::current().name().unwrap());
                })
                .unwrap());
        }

        let blob_counter = Arc::new(AtomicI64::new(0));
        let (blob_sender, blob_receiver) = bounded::<(Vec<u8>, usize)>(256);
        for i in 1..9 {
            let local_blob_receiver = blob_receiver.clone();
            let local_block_sender = block_sender.clone();
            let local_blob_counter = blob_counter.clone();
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
                        local_blob_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    info!("finished decompressing blobs ({})", thread::current().name().unwrap());
                })
                .unwrap());
        }

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

        let ticker = tick(Duration::from_millis(10000));
        while let Ok(_) = ticker.recv() {
            info!(
                "processed: nodes={}, ways={}, relations={}, blocks={}, blobs={}",
                node_counter.load(Ordering::Relaxed),
                way_counter.load(Ordering::Relaxed),
                relation_counter.load(Ordering::Relaxed),
                block_counter.load(Ordering::Relaxed),
                blob_counter.load(Ordering::Relaxed)
            );
            info!("queues: nodes={}, ways={}, relations={}, blocks={}, blobs={}", node_sender.len(), way_sender.len(), relation_sender.len(), block_sender.len(), blob_sender.len());
        }
    }

    for handle in handles.into_iter().rev() {
        handle.join().unwrap();
    }

    info!("Creating indexes...");
    let db = Connection::connect(db_url, TlsMode::None).unwrap();
    db.execute("ALTER TABLE node SET LOGGED", &[]).unwrap();
    db.execute("ALTER TABLE node ADD PRIMARY KEY (id)", &[]).unwrap();
    db.execute("ALTER TABLE way SET LOGGED", &[]).unwrap();
    db.execute("ALTER TABLE way ADD PRIMARY KEY (id)", &[]).unwrap();
    db.execute("ALTER TABLE relation SET LOGGED", &[]).unwrap();
    db.execute("ALTER TABLE relation ADD PRIMARY KEY (id)", &[]).unwrap();

    info!("Import complete.");
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
