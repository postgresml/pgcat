use log::{Level, Log, Metadata, Record, SetLoggerError};

// This is a special kind of logger that allows sending logs to different
// targets depending on the log level.
//
// By default, if nothing is set, it acts as a regular env_log logger,
// it sends everything to standard error.
//
// If the Env variable `STDOUT_LOG` is defined, it will be used for
// configuring the standard out logger.
//
// The behavior is:
//   - If it is an error, the message is written to standard error.
//   - If it is not, and it matches the log level of the standard output logger (`STDOUT_LOG` env var), it will be send to standard output.
//   - If the above is not true, it is sent to the stderr logger that will log it or not depending on the value
//     of the RUST_LOG env var.
//
// So to summarize, if no `STDOUT_LOG` env var is present, the logger is the default logger. If `STDOUT_LOG` is set, everything
// but errors, that matches the log level set in the `STDOUT_LOG` env var is sent to stdout. You can have also some esoteric configuration
// where you set `RUST_LOG=debug` and `STDOUT_LOG=info`, in here, errors will go to stderr, warns and infos to stdout and debugs to stderr.
//
pub struct MultiLogger {
    stderr_logger: env_logger::Logger,
    stdout_logger: env_logger::Logger,
}

impl MultiLogger {
    fn new() -> Self {
        let stderr_logger = env_logger::builder().format_timestamp_micros().build();
        let stdout_logger = env_logger::Builder::from_env("STDOUT_LOG")
            .format_timestamp_micros()
            .target(env_logger::Target::Stdout)
            .build();

        Self {
            stderr_logger,
            stdout_logger,
        }
    }

    pub fn init() -> Result<(), SetLoggerError> {
        let logger = Self::new();

        log::set_max_level(logger.stderr_logger.filter());
        log::set_boxed_logger(Box::new(logger))
    }
}

impl Log for MultiLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.stderr_logger.enabled(metadata) && self.stdout_logger.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if record.level() == Level::Error {
            self.stderr_logger.log(record);
        } else {
            if self.stdout_logger.matches(record) {
                self.stdout_logger.log(record);
            } else {
                self.stderr_logger.log(record);
            }
        }
    }

    fn flush(&self) {
        self.stderr_logger.flush();
        self.stdout_logger.flush();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_init() {
        MultiLogger::init().unwrap();
    }
}
