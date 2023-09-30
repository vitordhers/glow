use polars::prelude::*;
use std::env;
use std::fs;
use std::fs::{create_dir, metadata, File};
use std::path::Path;

use crate::trader::errors::{CustomError, Error};

pub fn save_csv(
    path: String,
    file_name: String,
    df: &DataFrame,
    overwrite: bool,
) -> Result<(), Error> {
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let _ = create_directories(&path);

    let file_path = format!("{}/{}/{}", current_dir.display(), &path, &file_name);

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
                    println!("File {} doesn't exist, creating it ...", file_path);
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
        let error = format!("File already exists, overwrite is set to false");
        let error = CustomError { message: error };
        return Err(Error::CustomError(error));
    }

    let mut df = df.clone();

    match File::create(file_path) {
        Ok(output_file) => {
            CsvWriter::new(output_file)
                .has_header(true)
                .with_float_precision(Some(6))
                .finish(&mut df)?;

            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn create_directories(path: &str) -> std::io::Result<()> {
    let mut cumulative_path = String::new();
    for segment in path.split('/') {
        if !cumulative_path.is_empty() {
            cumulative_path.push('/');
        }
        cumulative_path.push_str(segment);

        let path_obj = Path::new(&cumulative_path);
        if !path_obj.exists() {
            fs::create_dir_all(&path_obj)?;
        }
    }
    Ok(())
}
