use crate::db::error::StatusError::Corruption;
use crate::db::error::{Result, StatusError};
use crate::env::{write_string_to_file_sync, Env};

#[derive(Debug, PartialEq)]
pub enum FileType {
    LogFile,
    DBLockFile,
    TableFile,
    DescriptorFile,
    CurrentFile,
    TempFile,
    InfoLogFile,
}

fn make_file_name(dbname: &String, number: u64, suffix: &'static str) -> String {
    format!("{}/{:0>6}.{}", dbname, number, suffix)
}

pub fn log_file_name(dbname: &String, number: u64) -> String {
    assert!(number > 0);
    make_file_name(dbname, number, "log")
}

pub fn table_file_name(dbname: &String, number: u64) -> String {
    make_file_name(dbname, number, "ldb")
}

pub fn sst_table_file_name(dbname: &String, number: u64) -> String {
    make_file_name(dbname, number, "sst")
}

pub fn descriptor_file_name(dbname: &String, number: u64) -> String {
    format!("{}/MANIFEST-{:0>6}", dbname, number)
}

pub fn current_file_name(dbname: &String) -> String {
    format!("{}/CURRENT", dbname)
}

pub fn lock_file_name(dbname: &String) -> String {
    format!("{}/LOCK", dbname)
}

pub fn temp_file_name(dbname: &String, number: u64) -> String {
    assert!(number > 0);
    make_file_name(dbname, number, "dbtmp")
}

pub fn info_log_file_name(dbname: &String) -> String {
    format!("{}/LOG", dbname)
}

// Return the name of the old info log file for "dbname".
pub fn old_info_log_file_name(dbname: &String) -> String {
    format!("{}/LOG.old", dbname)
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
pub fn parse_file_name(file_name: &String) -> Result<(u64, FileType)> {
    let mut number = 0;
    let mut file_type = FileType::TempFile;

    let mut name = file_name.clone();
    if name.as_str() == "CURRENT" {
        number = 0;
        file_type = FileType::CurrentFile;
    } else if name.as_str() == "LOCK" {
        number = 0;
        file_type = FileType::DBLockFile;
    } else if name.as_str() == "LOG" || file_name.as_str() == "LOG.old" {
        number = 0;
        file_type = FileType::InfoLogFile;
    } else if name.starts_with("MANIFEST-") {
        let offset = "MANIFEST-".as_bytes().len();
        let mut rest: String = name.drain(offset..).collect();
        number = consume_decimal_number(&mut rest)?;
        if !rest.is_empty() {
            return Err(Corruption("file name illegal".to_string()));
        }
        file_type = FileType::DescriptorFile;
    } else {
        let mut rest = name.clone();
        number = consume_decimal_number(&mut rest)?;
        let suffix = rest.as_str();
        file_type = match suffix {
            ".log" => FileType::LogFile,
            ".sst" | ".ldb" => FileType::TableFile,
            ".dbtmp" => FileType::TempFile,
            _ => return Err(Corruption("file name illegal".to_string())),
        }
    }

    Ok((number, file_type))
}

pub fn set_current_file<E: Env>(env: E, dbname: &String, descriptor_number: u64) -> Result<()> {
    let manifest = descriptor_file_name(dbname, descriptor_number);
    let content = manifest[dbname.len() + 1..].to_string();
    let tmp = temp_file_name(dbname, descriptor_number);
    let res = write_string_to_file_sync(env.clone(), content.as_bytes(), &tmp);
    if res.is_ok() {
        env.rename_file(&tmp, &current_file_name(dbname))
    } else {
        env.delete_file(&tmp)
    }
}

fn consume_decimal_number(in_str: &mut String) -> Result<u64> {
    if let Some(pos) = in_str.rfind(char::is_numeric) {
        let number_str: String = in_str.drain(..pos + 1).collect();
        let ret = number_str.parse::<u64>()?;
        Ok(ret)
    } else {
        Err(StatusError::Corruption("have no number in str".to_string()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse() {
        // Successful parse
        let cases = vec![
            ("100.log", 100, FileType::LogFile),
            ("0.log", 0, FileType::LogFile),
            ("0.sst", 0, FileType::TableFile),
            ("0.ldb", 0, FileType::TableFile),
            ("CURRENT", 0, FileType::CurrentFile),
            ("LOCK", 0, FileType::DBLockFile),
            ("MANIFEST-2", 2, FileType::DescriptorFile),
            ("MANIFEST-7", 7, FileType::DescriptorFile),
            ("LOG", 0, FileType::InfoLogFile),
            ("LOG.old", 0, FileType::InfoLogFile),
            (
                "18446744073709551615.log",
                18446744073709551615,
                FileType::LogFile,
            ),
        ];

        for (name, number, file_type) in cases {
            let (parsed_number, parsed_type) = parse_file_name(&name.to_string()).unwrap();
            assert_eq!(parsed_number, number);
            assert_eq!(parsed_type, file_type);
        }

        // Error
        let cases = vec![
            "foo",
            "foo-dx-100.log",
            ".log",
            "",
            "manifest",
            "CURREN",
            "CURRENTX",
            "MANIFES",
            "MANIFEST",
            "MANIFEST-",
            "XMANIFEST-3",
            "MANIFEST-3x",
            "LOC",
            "LOCKx",
            "LO",
            "LOGx",
            "18446744073709551616.log",
            "184467440737095516150.log",
            "100",
            "100.",
            "100.lop",
        ];

        for case in cases {
            assert!(parse_file_name(&String::from(case)).is_err())
        }
    }

    #[test]
    fn test_construction() {
        let name = current_file_name(&"foo".to_string());
        assert_eq!("foo/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(0, number);
        assert_eq!(file_type, FileType::CurrentFile);

        let name = lock_file_name(&"foo".to_string());
        assert_eq!("foo/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(0, number);
        assert_eq!(file_type, FileType::DBLockFile);

        let name = log_file_name(&"foo".to_string(), 192);
        assert_eq!("foo/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(192, number);
        assert_eq!(file_type, FileType::LogFile);

        let name = table_file_name(&"bar".to_string(), 200);
        assert_eq!("bar/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(200, number);
        assert_eq!(file_type, FileType::TableFile);

        let name = descriptor_file_name(&"bar".to_string(), 100);
        assert_eq!("bar/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(100, number);
        assert_eq!(file_type, FileType::DescriptorFile);

        let name = temp_file_name(&"tmp".to_string(), 999);
        assert_eq!("tmp/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(999, number);
        assert_eq!(file_type, FileType::TempFile);

        let name = info_log_file_name(&"foo".to_string());
        assert_eq!("foo/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(0, number);
        assert_eq!(file_type, FileType::InfoLogFile);

        let name = old_info_log_file_name(&"foo".to_string());
        assert_eq!("foo/", &name[0..4]);
        let (number, file_type) = parse_file_name(&name[4..].to_string()).unwrap();
        assert_eq!(0, number);
        assert_eq!(file_type, FileType::InfoLogFile);
    }
}
