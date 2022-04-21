use super::scenario::{substitute_nanos, Scenario};
use crate::common::server_fixture::{ServerFixture, ServerType};

use futures::prelude::*;
use generated_types::{
    aggregate::AggregateType,
    google::protobuf::Empty,
    measurement_fields_response::FieldType,
    node::{Comparison, Type as NodeType, Value},
    offsets_response::PartitionOffsetResponse,
    read_group_request::Group,
    read_response::{frame::Data, *},
    storage_client::StorageClient,
    Aggregate, MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, OffsetsResponse, Predicate, ReadFilterRequest,
    ReadGroupRequest, ReadWindowAggregateRequest, Tag, TagKeysRequest, TagValuesRequest,
    TimestampRange,
};
use influxdb_iox_client::connection::Connection;
use influxdb_storage_client::tag_key_bytes_to_strings;
use std::str;

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;

    let mut write_client = server_fixture.write_client();
    let mut storage_client = StorageClient::new(server_fixture.grpc_channel());
    let mut management_client = server_fixture.management_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;
    scenario.load_data(&mut write_client).await;

    read_filter_endpoint(&mut storage_client, &scenario).await;
    tag_keys_endpoint(&mut storage_client, &scenario).await;
    tag_values_endpoint(&mut storage_client, &scenario).await;
    measurement_names_endpoint(&mut storage_client, &scenario).await;
    measurement_tag_keys_endpoint(&mut storage_client, &scenario).await;
    measurement_tag_values_endpoint(&mut storage_client, &scenario).await;
    measurement_fields_endpoint(&mut storage_client, &scenario).await;
}

/// Validate that capabilities storage endpoint is hooked up
#[tokio::test]
async fn capabilities_endpoint() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut storage_client = StorageClient::new(server_fixture.grpc_channel());

    let capabilities_response = storage_client
        .capabilities(Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        capabilities_response.caps.len(),
        3,
        "Response: {:?}",
        capabilities_response
    );
}

/// Validate that storage offsets endpoint is hooked up (required by internal Influx cloud)
#[tokio::test]
async fn offsets_endpoint() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut storage_client = StorageClient::new(server_fixture.grpc_channel());

    let offsets_response = storage_client.offsets(Empty {}).await.unwrap().into_inner();
    let expected = OffsetsResponse {
        partitions: vec![PartitionOffsetResponse { id: 0, offset: 1 }],
    };
    assert_eq!(offsets_response, expected);
}

async fn read_filter_endpoint(storage_client: &mut StorageClient<Connection>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source,
        range,
        predicate,
        ..Default::default()
    });
    let read_response = storage_client
        .read_filter(read_filter_request)
        .await
        .unwrap();

    let responses: Vec<_> = read_response.into_inner().try_collect().await.unwrap();
    let frames: Vec<Data> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    let expected_frames = substitute_nanos(scenario.ns_since_epoch(), &[
        "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,_field=value, type: 0",
        "FloatPointsFrame, timestamps: [ns1], values: \"27.99\"",
        "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,region=us-east,_field=value, type: 0",
        "FloatPointsFrame, timestamps: [ns3], values: \"1234567.891011\"",
        "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,region=us-west,_field=value, type: 0",
        "FloatPointsFrame, timestamps: [ns0, ns4], values: \"0.64,0.000003\"",
        "SeriesFrame, tags: _measurement=swap,host=server01,name=disk0,_field=in, type: 1",
        "IntegerPointsFrame, timestamps: [ns6], values: \"3\"",
        "SeriesFrame, tags: _measurement=swap,host=server01,name=disk0,_field=out, type: 1",
        "IntegerPointsFrame, timestamps: [ns6], values: \"4\""
    ]);

    let actual_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_frames,
        actual_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_frames.join("\n"),
        actual_frames.join("\n")
    );
}

async fn tag_keys_endpoint(storage_client: &mut StorageClient<Connection>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();
    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let tag_keys_request = tonic::Request::new(TagKeysRequest {
        tags_source: read_source,
        range,
        predicate,
    });

    let tag_keys_response = storage_client.tag_keys(tag_keys_request).await.unwrap();
    let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await.unwrap();

    let keys = &responses[0].values;
    let keys: Vec<_> = keys
        .iter()
        .map(|v| tag_key_bytes_to_strings(v.clone()))
        .collect();

    assert_eq!(keys, vec!["_m(0x00)", "host", "name", "region", "_f(0xff)"]);
}

async fn tag_values_endpoint(storage_client: &mut StorageClient<Connection>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();
    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let tag_values_request = tonic::Request::new(TagValuesRequest {
        tags_source: read_source,
        range,
        predicate,
        tag_key: b"host".to_vec(),
    });

    let tag_values_response = storage_client.tag_values(tag_values_request).await.unwrap();
    let responses: Vec<_> = tag_values_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let values = &responses[0].values;
    let values: Vec<_> = values
        .iter()
        .map(|v| tag_key_bytes_to_strings(v.clone()))
        .collect();

    assert_eq!(values, vec!["server01"]);
}

async fn measurement_names_endpoint(
    storage_client: &mut StorageClient<Connection>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let measurement_names_request = tonic::Request::new(MeasurementNamesRequest {
        source: read_source,
        range,
        predicate: None,
    });

    let measurement_names_response = storage_client
        .measurement_names(measurement_names_request)
        .await
        .unwrap();
    let responses: Vec<_> = measurement_names_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(
        values,
        vec!["attributes", "cpu_load_short", "status", "swap", "system"]
    );
}

async fn measurement_tag_keys_endpoint(
    storage_client: &mut StorageClient<Connection>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_tag_keys_request = tonic::Request::new(MeasurementTagKeysRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        range,
        predicate,
    });

    let measurement_tag_keys_response = storage_client
        .measurement_tag_keys(measurement_tag_keys_request)
        .await
        .unwrap();
    let responses: Vec<_> = measurement_tag_keys_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let values = &responses[0].values;
    let values: Vec<_> = values
        .iter()
        .map(|v| tag_key_bytes_to_strings(v.clone()))
        .collect();

    assert_eq!(values, vec!["_m(0x00)", "host", "region", "_f(0xff)"]);
}

async fn measurement_tag_values_endpoint(
    storage_client: &mut StorageClient<Connection>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_tag_values_request = tonic::Request::new(MeasurementTagValuesRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        tag_key: String::from("host"),
        range,
        predicate,
    });

    let measurement_tag_values_response = storage_client
        .measurement_tag_values(measurement_tag_values_request)
        .await
        .unwrap();
    let responses: Vec<_> = measurement_tag_values_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let values = &responses[0].values;
    let values: Vec<_> = values
        .iter()
        .map(|v| tag_key_bytes_to_strings(v.clone()))
        .collect();

    assert_eq!(values, vec!["server01"]);
}

async fn measurement_fields_endpoint(
    storage_client: &mut StorageClient<Connection>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_fields_request = tonic::Request::new(MeasurementFieldsRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        range,
        predicate,
    });

    let measurement_fields_response = storage_client
        .measurement_fields(measurement_fields_request)
        .await
        .unwrap();
    let responses: Vec<_> = measurement_fields_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let fields = &responses[0].fields;
    assert_eq!(fields.len(), 1);

    let field = &fields[0];
    assert_eq!(field.key, "value");
    assert_eq!(field.r#type(), FieldType::Float);
    assert_eq!(field.timestamp, scenario.ns_since_epoch() + 4);
}

#[tokio::test]
pub async fn read_filter_regex_operator() {
    do_read_filter_test(
        read_group_data(),
        TimestampRange {
            start: 0,
            end: 2001, // include all data
        },
        make_regex_match_predicate("host", "^b.+"),
        vec![
            "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
            "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_user, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
            "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
            "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_user, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_eq() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        TimestampRange {
            start: 0,
            end: 2001, // include all data
        },
        // https://github.com/influxdata/influxdb_iox/issues/3430
        // host = '' means where host is not present
        make_tag_predicate("host", ""),
        vec![
            "SeriesFrame, tags: _measurement=cpu,_field=value, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_not_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        TimestampRange {
            start: 0,
            end: 2001, // include all data
        },
        // https://github.com/influxdata/influxdb_iox/issues/3434
        // host !~ /^server01$/ means where host doesn't start with `server01`
        make_not_regex_match_predicate("host", "^server01"),
        vec![
            "SeriesFrame, tags: _measurement=cpu,_field=value, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        TimestampRange {
            start: 0,
            end: 2001, // include all data
        },
        // host =~ /.+/ means where host is at least one character
        make_regex_match_predicate("host", ".+"),
        vec![
            "SeriesFrame, tags: _measurement=cpu,host=server01,_field=value, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"2\"",
        ],
    )
    .await
}

//////// Tests above here have been ported to NG. Tests below have not yet been /////

/// Creates and loads the common data for read_group
async fn read_group_setup() -> (ServerFixture, Scenario) {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management = fixture.management_client();
    let mut write = fixture.write_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management).await;

    let line_protocol = read_group_data().join("\n");

    scenario
        .write_data(&mut write, line_protocol)
        .await
        .expect("Wrote cpu line protocol data");

    (fixture, scenario)
}

fn read_group_data() -> Vec<&'static str> {
    vec![
        "cpu,cpu=cpu1,host=foo  usage_user=71.0,usage_system=10.0 1000",
        "cpu,cpu=cpu1,host=foo  usage_user=72.0,usage_system=11.0 2000",
        "cpu,cpu=cpu1,host=bar  usage_user=81.0,usage_system=20.0 1000",
        "cpu,cpu=cpu1,host=bar  usage_user=82.0,usage_system=21.0 2000",
        "cpu,cpu=cpu2,host=foo  usage_user=61.0,usage_system=30.0 1000",
        "cpu,cpu=cpu2,host=foo  usage_user=62.0,usage_system=31.0 2000",
        "cpu,cpu=cpu2,host=bar  usage_user=51.0,usage_system=40.0 1000",
        "cpu,cpu=cpu2,host=bar  usage_user=52.0,usage_system=41.0 2000",
    ]
}

/// Standalone test for read_group with group keys and no aggregate
/// assumes that read_group_data has been previously loaded
#[tokio::test]
async fn test_read_group_none_agg() {
    let (fixture, scenario) = read_group_setup().await;
    let mut storage_client = fixture.storage_client();

    // read_group(group_keys: region, agg: None)
    let read_group_request = ReadGroupRequest {
        read_source: scenario.read_source(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::None as i32,
        }),
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu1",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"10,11\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"71,72\"",
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu2",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"30,31\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"61,62\"",
    ];

    let actual_group_frames = do_read_group_request(&mut storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames, actual_group_frames,
        "Expected:\n{:#?}\nActual:\n{:#?}",
        expected_group_frames, actual_group_frames
    );
}

/// Test that predicates make it through
#[tokio::test]
async fn test_read_group_none_agg_with_predicate() {
    let (fixture, scenario) = read_group_setup().await;
    let mut storage_client = fixture.storage_client();

    let read_group_request = ReadGroupRequest {
        read_source: scenario.read_source(),
        range: Some(TimestampRange {
            start: 0,
            end: 2000, // do not include data at timestamp 2000
        }),
        predicate: Some(make_field_predicate("usage_system")),
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::None as i32,
        }),
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu1",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"20\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"10\"",
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu2",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"40\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"30\"",
    ];

    let actual_group_frames = do_read_group_request(&mut storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames, actual_group_frames,
        "Expected:\n{:#?}\nActual:\n{:#?}",
        expected_group_frames, actual_group_frames
    );
}

// Standalone test for read_group with group keys and an actual
// "aggregate" (not a "selector" style).  assumes that
// read_group_data has been previously loaded
#[tokio::test]
async fn test_read_group_sum_agg() {
    let (fixture, scenario) = read_group_setup().await;
    let mut storage_client = fixture.storage_client();

    // read_group(group_keys: region, agg: Sum)
    let read_group_request = ReadGroupRequest {
        read_source: scenario.read_source(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Sum as i32,
        }),
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu1",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"41\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"163\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"21\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"143\"",
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu2",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"81\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"103\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"61\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"123\"",
    ];

    let actual_group_frames = do_read_group_request(&mut storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames, actual_group_frames,
        "Expected:\n{:#?}\nActual:\n{:#?}",
        expected_group_frames, actual_group_frames,
    );
}

// Standalone test for read_group with group keys the count aggregate
// (returns a different type than the field types)
#[tokio::test]
async fn test_read_group_count_agg() {
    let (fixture, scenario) = read_group_setup().await;
    let mut storage_client = fixture.storage_client();

    // read_group(group_keys: region, agg: Count)
    let read_group_request = ReadGroupRequest {
        read_source: scenario.read_source(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Count as i32,
        }),
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu1",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_user, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_system, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_user, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu2",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_user, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_system, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_user, type: 1",
        "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
    ];

    let actual_group_frames = do_read_group_request(&mut storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames, actual_group_frames,
        "Expected:\n{:#?}\nActual:\n{:#?}",
        expected_group_frames, actual_group_frames,
    );
}

// Standalone test for read_group with group keys and an actual
// "selector" function last.  assumes that
// read_group_data has been previously loaded
#[tokio::test]
async fn test_read_group_last_agg() {
    let (fixture, scenario) = read_group_setup().await;
    let mut storage_client = fixture.storage_client();

    // read_group(group_keys: region, agg: Last)
    let read_group_request = ReadGroupRequest {
        read_source: scenario.read_source(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Last as i32,
        }),
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu1",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"21\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"82\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"11\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu1,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"72\"",
        "GroupFrame, tag_keys: _measurement,cpu,host,_field, partition_key_vals: cpu2",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"41\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=bar,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"52\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_system, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"31\"",
        "SeriesFrame, tags: _measurement=cpu,cpu=cpu2,host=foo,_field=usage_user, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"62\"",
    ];

    let actual_group_frames = do_read_group_request(&mut storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames, actual_group_frames,
        "Expected:\n{:#?}\nActual:\n{:#?}",
        expected_group_frames, actual_group_frames,
    );
}

// Standalone test that all the pipes are hooked up for read window aggregate
#[tokio::test]
pub async fn read_window_aggregate_test() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management = fixture.management_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let mut write = fixture.write_client();

    let scenario = Scenario::new();
    let read_source = scenario.read_source();

    scenario.create_database(&mut management).await;

    let line_protocol = vec![
        "h2o,state=MA,city=Boston temp=70.0 100",
        "h2o,state=MA,city=Boston temp=71.0 200",
        "h2o,state=MA,city=Boston temp=72.0 300",
        "h2o,state=MA,city=Boston temp=73.0 400",
        "h2o,state=MA,city=Boston temp=74.0 500",
        "h2o,state=MA,city=Cambridge temp=80.0 100",
        "h2o,state=MA,city=Cambridge temp=81.0 200",
        "h2o,state=MA,city=Cambridge temp=82.0 300",
        "h2o,state=MA,city=Cambridge temp=83.0 400",
        "h2o,state=MA,city=Cambridge temp=84.0 500",
        "h2o,state=CA,city=LA temp=90.0 100",
        "h2o,state=CA,city=LA temp=91.0 200",
        "h2o,state=CA,city=LA temp=92.0 300",
        "h2o,state=CA,city=LA temp=93.0 400",
        "h2o,state=CA,city=LA temp=94.0 500",
    ]
    .join("\n");

    write
        .write_lp(&*scenario.database_name(), line_protocol, 0)
        .await
        .expect("Wrote h20 line protocol");

    // now, query using read window aggregate

    let request = ReadWindowAggregateRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 200,
            end: 1000,
        }),
        predicate: Some(make_tag_predicate("state", "MA")),
        window_every: 200,
        offset: 0,
        aggregate: vec![Aggregate {
            r#type: AggregateType::Sum as i32,
        }],
        window: None,
    };

    let response = storage_client.read_window_aggregate(request).await.unwrap();

    let responses: Vec<_> = response.into_inner().try_collect().await.unwrap();

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    let expected_frames = vec![
        "SeriesFrame, tags: _measurement=h2o,city=Boston,state=MA,_field=temp, type: 0",
        "FloatPointsFrame, timestamps: [400, 600], values: \"143,147\"",
        "SeriesFrame, tags: _measurement=h2o,city=Cambridge,state=MA,_field=temp, type: 0",
        "FloatPointsFrame, timestamps: [400, 600], values: \"163,167\"",
    ];

    let actual_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_frames,
        actual_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_frames.join("\n"),
        actual_frames.join("\n")
    );
}

/// Sends the specified line protocol to a server with the timestamp/ predicate
/// predicate, and compares it against expected frames
async fn do_read_filter_test(
    input_lines: Vec<&str>,
    range: TimestampRange,
    predicate: Predicate,
    expected_frames: Vec<&str>,
) {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management = fixture.management_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let mut write_client = fixture.write_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management).await;

    let line_protocol = input_lines.join("\n");

    scenario
        .write_data(&mut write_client, line_protocol)
        .await
        .expect("Wrote line protocol data");

    let read_source = scenario.read_source();

    println!(
        "Sending read_filter request with range {:?} and predicate {:?}",
        range, predicate
    );

    let read_filter_request = ReadFilterRequest {
        read_source: read_source.clone(),
        range: Some(range),
        predicate: Some(predicate),
        ..Default::default()
    };

    assert_eq!(
        do_read_filter_request(&mut storage_client, read_filter_request).await,
        expected_frames,
    );
}

/// Create a predicate representing tag_name=tag_value in the horrible gRPC
/// structs
fn make_tag_predicate(tag_name: impl Into<String>, tag_value: impl Into<String>) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(tag_value.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}

/// Create a predicate representing tag_name ~= /pattern/
fn make_regex_match_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
) -> Predicate {
    make_regex_predicate(tag_key_name, pattern, Comparison::Regex)
}

/// Create a predicate representing tag_name !~ /pattern/
fn make_not_regex_match_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
) -> Predicate {
    make_regex_predicate(tag_key_name, pattern, Comparison::NotRegex)
}

/// Create a predicate representing tag_name <op> /pattern/
///
/// where op is `Regex` or `NotRegEx`
/// The constitution of this request was formed by looking at a real request
/// made to storage, which looked like this:
///
/// root:<
///         node_type:COMPARISON_EXPRESSION
///         children:<node_type:TAG_REF tag_ref_value:"tag_key_name" >
///         children:<node_type:LITERAL regex_value:"pattern" >
///         comparison:REGEX
/// >
fn make_regex_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
    comparison: Comparison,
) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_key_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::RegexValue(pattern.into())),
                },
            ],
            value: Some(Value::Comparison(comparison as _)),
        }),
    }
}

/// Create a predicate representing _f=field_name in the horrible gRPC structs
fn make_field_predicate(field_name: impl Into<String>) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue([255].to_vec())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(field_name.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}

/// Make a read_group request and returns the results in a comparable format
async fn do_read_filter_request(
    storage_client: &mut StorageClient<Connection>,
    request: ReadFilterRequest,
) -> Vec<String> {
    let request = tonic::Request::new(request);

    let read_filter_response = storage_client
        .read_filter(request)
        .await
        .expect("successful read_filter call");

    let responses: Vec<_> = read_filter_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    dump_data_frames(&frames)
}

/// Make a read_group request and returns the results in a comparable format
async fn do_read_group_request(
    storage_client: &mut StorageClient<Connection>,
    request: ReadGroupRequest,
) -> Vec<String> {
    let request = tonic::Request::new(request);

    let read_group_response = storage_client
        .read_group(request)
        .await
        .expect("successful read_group call");

    let responses: Vec<_> = read_group_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    dump_data_frames(&frames)
}

fn dump_data_frames(frames: &[Data]) -> Vec<String> {
    frames.iter().map(dump_data).collect()
}

fn dump_data(data: &Data) -> String {
    match Some(data) {
        Some(Data::Series(SeriesFrame { tags, data_type })) => format!(
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => format!(
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => format!(
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => format!(
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => format!(
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::UnsignedPoints(UnsignedPointsFrame { timestamps, values })) => format!(
            "UnsignedPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => format!(
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals),
        ),
        None => "<NO data field>".into(),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{}", item))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_u8_vec(encoded_strings: &[Vec<u8>]) -> String {
    encoded_strings
        .iter()
        .map(|b| String::from_utf8_lossy(b))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&tag.key),
                String::from_utf8_lossy(&tag.value),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
