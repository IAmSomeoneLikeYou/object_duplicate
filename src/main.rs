use anyhow::{Ok, Result};
use aws_config::{meta::region::RegionProviderChain, retry::RetryConfig};
use aws_sdk_s3 as s3;
use aws_sdk_s3::Client;
use rand::{thread_rng, Rng};
use url::Url;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Get the directory path from command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <s3_uri> <object_number>", args[0]);
        return Ok(());
    }
    let s3_uri = &args[1];
    let url = Url::parse(&s3_uri).unwrap();

    let object_str = &args[2];
    let object_num = object_str.parse::<usize>().unwrap();


    let bucket = Arc::new(url.host_str().unwrap().to_string());

    let key = url.path().trim_start_matches('/').to_owned();



    let prefix_str = match key.rsplitn(2, "/").nth(1){
        Some(p) => p.to_string(),
        None => "".to_string()
    };
   
    let prefix = Arc::new(prefix_str);
    let object_name = Arc::new(key.rsplitn(2, "/").nth(0).unwrap().to_string());



    let num_cores = num_cpus::get();
    let mut handles = vec![];
    for _ in 0..num_cores {
        let bucket_clone = Arc::clone(&bucket);
        let prefix_clone =Arc::clone(&prefix);
        let object_name_clone = Arc::clone(&object_name);

        let handle = tokio::spawn(async move {
            let client = create_3_client(3).await.unwrap();
            let file_num_per_thread = object_num / num_cores;
            for _ in 0..file_num_per_thread {
                let mut old_key = prefix_clone.to_string();
                if !old_key.is_empty(){
                    old_key.push_str("/");

                }
                old_key.push_str(&object_name_clone);
                let random_num: u128 = thread_rng().gen();
                let new_object_name = random_num.to_string();
                let mut new_key = prefix_clone.to_string();
                if !new_key.is_empty(){
                    new_key.push_str("/");
                }
                new_key.push_str("new_json_");
                new_key.push_str(&new_object_name); 
                copy_object(&client, (*bucket_clone).as_str(), &old_key, &new_key).await.unwrap();
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

//creat a s3 client
pub async fn create_3_client(retries: u32) -> anyhow::Result<s3::Client> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let retry_config = RetryConfig::standard().with_max_attempts(retries);
    let config = aws_sdk_s3::config::Builder::from(&shared_config)
        .retry_config(retry_config)
        .build();
    let client = s3::Client::from_conf(config);

    Ok(client)
}

pub async fn copy_object(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    target_key: &str,
) -> Result<()> {
    let mut source_bucket_and_object: String = "".to_owned();
    source_bucket_and_object.push_str(bucket_name);
    source_bucket_and_object.push('/');
    source_bucket_and_object.push_str(object_key);

    client
        .copy_object()
        .copy_source(source_bucket_and_object)
        .bucket(bucket_name)
        .key(target_key)
        .send()
        .await?;
    Ok(())
}
