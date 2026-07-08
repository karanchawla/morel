use futures_util::StreamExt;
use morel::{
    produce_async_with, AsyncIoConfig, AsyncIoError, AsyncIoRuntime, AsyncProducer, Capacity,
    Graph, Live, OnClose, Stop,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn default_config_is_bounded_graph_local_with_5s_shutdown() {
    let config = AsyncIoConfig::default();

    assert_eq!(config.capacity, Capacity::Bounded(1024));
    assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
    assert!(matches!(config.runtime, AsyncIoRuntime::GraphLocal));
}

#[test]
fn config_builders_override_each_field() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = AsyncIoConfig::default()
        .with_capacity(Capacity::Unbounded)
        .with_shutdown_timeout(Duration::from_millis(100))
        .with_runtime(AsyncIoRuntime::Handle(rt.handle().clone()));

    assert_eq!(config.capacity, Capacity::Unbounded);
    assert_eq!(config.shutdown_timeout, Duration::from_millis(100));
    assert!(matches!(config.runtime, AsyncIoRuntime::Handle(_)));
}

#[test]
fn errors_have_actionable_messages() {
    assert_eq!(
        AsyncIoError::task("async producer", "connect refused").to_string(),
        "async producer failed: connect refused"
    );
    assert_eq!(
        AsyncIoError::LiveOnly {
            op: "async producer"
        }
        .to_string(),
        "async producer can only run in live mode"
    );
    assert_eq!(
        AsyncIoError::Closed {
            op: "async consumer"
        }
        .to_string(),
        "async consumer channel is closed"
    );
    assert_eq!(
        AsyncIoError::NestedRuntime {
            op: "async producer"
        }
        .to_string(),
        "async producer cannot create a graph-local runtime inside an async \
         context; inject AsyncIoRuntime::Handle or AsyncIoRuntime::Runtime"
    );
}

#[test]
fn injected_handle_runs_nodes_inside_async_context() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let g = Graph::new();
        let config = AsyncIoConfig::default()
            .with_runtime(AsyncIoRuntime::Handle(tokio::runtime::Handle::current()));
        let out = produce_async_with(
            &g,
            config.clone(),
            OnClose::Continue,
            |_params, p: AsyncProducer<i64>| async move {
                p.send(42).await?;
                Ok::<(), AsyncIoError>(())
            },
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let _sink = out.consume_async_with(config, move |_params, mut input| async move {
            while let Some((_time, burst)) = input.next().await {
                seen2.lock().unwrap().extend(burst);
            }
            Ok::<(), AsyncIoError>(())
        });

        g.run(Live::new().stop(Stop::After(Duration::from_millis(50))))
            .unwrap();

        assert_eq!(*seen.lock().unwrap(), vec![42]);
    });
}
