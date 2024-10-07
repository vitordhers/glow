use crate::{functions::get_days_between, structs::Symbol};
use chrono::{NaiveDate, NaiveDateTime};
use glow_error::{assert_or_error, GlowError};
use polars::prelude::*;
use std::{
    env,
    fs::{create_dir, create_dir_all, metadata, File},
    path::{Path, PathBuf},
};

pub fn save_csv(
    path: String,
    file_name: String,
    df: &DataFrame,
    overwrite: bool,
) -> Result<(), GlowError> {
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let _ = create_file_directories(&path);

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
        return Err(GlowError::new(
            String::from("Save .csv error"),
            format!("File already exists, overwrite is set to false"),
        ));
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

pub fn save_kline_df_to_csv(
    df: &DataFrame,
    date: NaiveDate,
    data_provider_exchange_name: &str,
    symbol_name: &str,
) -> Result<(), GlowError> {
    let file_path = get_tick_data_csv_path(date, data_provider_exchange_name, symbol_name);
    let mut folder_path = file_path.clone();
    folder_path.pop();

    let file_exists = match metadata(folder_path) {
        Ok(path_metadata) => {
            if !path_metadata.is_dir() {
                panic!("Path exists, but it is not a directory");
            }
            let file_path_metadata_result = metadata(&file_path);
            let result = match file_path_metadata_result {
                Ok(file_path_metadata) => file_path_metadata.is_file(),
                Err(_) => {
                    println!("File {:?} doesn't exist, creating it ...", file_path);
                    false
                }
            };
            result
        }
        Err(_) => {
            match create_file_directories(file_path.to_str().unwrap()) {
                Ok(_) => {}
                Err(error) => panic!("Failed to create directory: {}", error),
            }
            false
        }
    };
    if file_exists {
        return Ok(());
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
        Err(error) => {
            println!("save_kline_df_to_csv error {:?}", error);
            Err(error.into())
        }
    }
}

pub fn get_tick_data_csv_path(
    date: NaiveDate,
    data_provider_exchange_name: &str,
    symbol_name: &str,
) -> PathBuf {
    let mut path_buf = PathBuf::from("data/ticks");
    path_buf.push(data_provider_exchange_name);
    path_buf.push(symbol_name);
    path_buf.push(date.format("%Y").to_string());
    path_buf.push(date.format("%m").to_string());
    path_buf.push(format!("{}{}", date.format("%d").to_string(), ".csv"));
    path_buf
}

/// Tries to load ticks data between an interval. If a file is absent, push the NaiveDates of absent dates
pub fn load_interval_tick_dataframe(
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
    symbol: &Symbol,
    data_provider_exchange_name: &str,
) -> Result<(Option<DataFrame>, Vec<NaiveDate>), GlowError> {
    assert_or_error!(start_datetime <= end_datetime);
    let days_between = get_days_between(start_datetime, end_datetime)?;
    let mut dataframe = None;
    let mut not_loaded_dates = vec![];
    let symbol_tick_data_schema = symbol.derive_symbol_tick_data_schema();

    for date in days_between {
        let path = get_tick_data_csv_path(date, data_provider_exchange_name, &symbol.name);
        match load_csv(path, &symbol_tick_data_schema) {
            Ok(df) => {
                if dataframe.is_none() {
                    dataframe = Some(df);
                } else {
                    dataframe = Some(dataframe.unwrap().vstack(&df)?);
                }
            }
            Err(error) => {
                println!("LOAD CSV ERROR {:?}", error);
                not_loaded_dates.push(date);
            }
        }
    }

    Ok((dataframe, not_loaded_dates))
}

pub fn load_csv<P: Into<PathBuf>>(path: P, schema: &Schema) -> Result<DataFrame, PolarsError> {
    CsvReader::from_path(path)?
        .has_header(true)
        .with_schema(Some(Arc::new(schema.clone())))
        .finish()
}

fn create_file_directories(file_path: &str) -> Result<(), GlowError> {
    let mut cumulative_path = String::new();
    for segment in file_path.split('/') {
        if segment.contains(".") {
            break;
        }
        if !cumulative_path.is_empty() {
            cumulative_path.push('/');
        }
        cumulative_path.push_str(segment);

        let path_obj = Path::new(&cumulative_path);
        if !path_obj.exists() {
            create_dir_all(&path_obj)?;
        }
    }
    Ok(())
}

pub fn get_current_env_log_path() -> String {
    let env = env::var("ENV_NAME").unwrap().to_uppercase();
    if env == "PROD".to_string() {
        "data/journals".to_string()
    } else {
        "data/test".to_string()
    }
}
