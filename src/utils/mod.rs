use std::path::Path;

pub fn is_valid_source(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);
            path.is_file() && path.exists()
        },
        _ => false,
    }
}

pub fn is_valid_destination(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            if is_valid_directory_path(Some(to)) {
                true
            } else {
                has_valid_parent_dir(Some(to)) && has_same_extension(Some(from), Some(to))
            }
        },
        _ => false,
    }
}

pub fn is_valid_directory_path(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);
            path.exists() && path.is_dir()
        },
        _ => false,
    }
}

pub fn has_valid_parent_dir(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);
            if let Some(parent) = path.parent() {
                is_valid_directory_path(parent.to_str())
            } else {
                false
            }
        },
        _ => false,
    }
}

pub fn has_same_extension(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) if !to.is_empty() => {
            let from_path = Path::new(from);
            let to_path = Path::new(to);

            if let (Some(from_ext), Some(to_ext)) = (from_path.extension(), to_path.extension()) {
                from_ext == to_ext
            } else {
                false
            }
        },
        _ => false,
    }
}
