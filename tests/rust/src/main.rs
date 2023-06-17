#[tokio::main]
async fn main() {
    test_prepared_statements().await;
}

async fn test_prepared_statements() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db")
        .await
        .unwrap();

    let mut handles = Vec::new();

    for _ in 0..5 {
        let pool = pool.clone();
        let handle = tokio::task::spawn(async move {
            for _ in 0..1000 {
                sqlx::query("SELECT 1").fetch_all(&pool).await.unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
