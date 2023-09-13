use polars::prelude::*;
use std::env;
use std::fs::{create_dir, metadata, File};

use crate::trader::errors::Error;

pub fn save_csv(
    path: String,
    file_name: String,
    df: &DataFrame,
    overwrite: bool,
) -> Result<(), Error> {
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let file_path = format!("{}/{}/{}", current_dir.display(), &path, &file_name);
    // println!("Saving {:?} csv...", &file_path);

    let path_metadata_result = metadata(&path);

    let file_exists = match path_metadata_result {
        Ok(path_metadata) => {
            if !path_metadata.is_dir() {
                panic!("Path exists, but it is not a directory");
            }
            let file_path_metadata_result = metadata(&file_path);
            let result = match file_path_metadata_result {
                Ok(file_path_metadata) => file_path_metadata.is_file(),
                Err(_) => {
                    println!("File {} doesn't exist", file_path);
                    false
                }
            };
            result
        }
        Err(_) => {
            println!("Directory {} doesn't exist, creating it ...", &path);
            match create_dir(&path) {
                Ok(()) => {
                    println!("Directory {} created", path);
                }
                Err(error) => panic!("Failed to create directory: {}", error),
            }
            false
        }
    };

    if file_exists & !overwrite {
        panic!("File already exists, overwrite is set to false");
    }

    let mut df = df.clone();

    let output_file: File = File::create(file_path).unwrap();

    CsvWriter::new(output_file)
        .has_header(true)
        .with_float_precision(Some(6))
        .finish(&mut df)?;

    Ok(())
}
