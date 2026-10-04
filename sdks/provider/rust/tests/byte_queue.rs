use mppi_provider::queue::{ByteQueue, QueueError};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn byte_and_message_bounds_timeout_cancellation_and_terminal_drain() {
    let queue = Arc::new(ByteQueue::new(2, 4, 10).unwrap());
    queue.put_nowait(b"1234").unwrap();
    assert!(matches!(queue.put_nowait(b"x"), Err(QueueError::Full)));
    assert!(matches!(
        queue.put_nowait(b"12345"),
        Err(QueueError::TooLarge)
    ));
    assert!(matches!(
        queue.put(b"x".to_vec()).await,
        Err(QueueError::TimedOut)
    ));
    let clone = queue.clone();
    let blocked = tokio::spawn(async move { clone.put(b"y".to_vec()).await });
    tokio::task::yield_now().await;
    blocked.abort();
    assert!(blocked.await.unwrap_err().is_cancelled());
    assert_eq!(queue.buffered().unwrap(), (1, 4));
    assert_eq!(queue.get().await.unwrap(), b"1234");
    queue.put_nowait(b"z").unwrap();
    queue
        .close(Some(tonic::Status::unavailable("test close")))
        .unwrap();
    queue.close(None).unwrap();
    assert_eq!(queue.get().await.unwrap(), b"z");
    assert!(
        matches!(queue.get().await, Err(QueueError::Failed(status)) if status.code() == tonic::Code::Unavailable)
    );
    assert!(matches!(queue.put_nowait(b""), Err(QueueError::Closed)));
    let count = ByteQueue::new(1, 10, 10).unwrap();
    count.put_nowait(b"").unwrap();
    assert!(matches!(count.put_nowait(b""), Err(QueueError::Full)));
}

#[tokio::test]
async fn close_wakes_all_blocked_consumers_and_producers_without_lost_notifications() {
    let queue = Arc::new(ByteQueue::new(1, 4, 1000).unwrap());
    let first = queue.clone();
    let second = queue.clone();
    let a = tokio::spawn(async move { first.get().await });
    let b = tokio::spawn(async move { second.get().await });
    tokio::task::yield_now().await;
    queue.put_nowait(b"1").unwrap();
    tokio::task::yield_now().await;
    queue.put(b"2".to_vec()).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), async {
        (a.await.unwrap().unwrap(), b.await.unwrap().unwrap())
    })
    .await
    .unwrap();
    assert!(
        received == (b"1".to_vec(), b"2".to_vec()) || received == (b"2".to_vec(), b"1".to_vec())
    );
    let closed = queue.clone();
    let pending = tokio::spawn(async move { closed.get().await });
    tokio::task::yield_now().await;
    queue.close(None).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap(),
        Err(QueueError::Closed)
    ));
    let full = Arc::new(ByteQueue::new(1, 4, 1000).unwrap());
    full.put_nowait(b"full").unwrap();
    let cloned = full.clone();
    let pending = tokio::spawn(async move { cloned.put(b"next".to_vec()).await });
    tokio::task::yield_now().await;
    full.close(None).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap(),
        Err(QueueError::Closed)
    ));
}
