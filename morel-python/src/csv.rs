use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList};

use crate::error::{callback_error, type_error};
use crate::graph::PyGraph;
use crate::stream::{PyStream, PyStreamOwner};
use crate::value::PyValue;

type CsvRecords = ::csv::StringRecordsIntoIter<File>;
type CsvItem = Result<(morel::Time, PyValue), Box<dyn Error + Send + Sync>>;

struct PyCsvReplay {
    path: PathBuf,
    shown: String,
    parse: Py<PyAny>,
    records: Option<CsvRecords>,
    pending: Option<CsvItem>,
    out: morel::Output<PyValue>,
}

impl PyCsvReplay {
    fn open(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let reader =
            ::csv::Reader::from_path(&self.path).map_err(|err| format!("{}: {err}", self.shown))?;
        self.records = Some(reader.into_records());
        Ok(())
    }

    fn ensure_pending(&mut self) {
        if self.pending.is_some() {
            return;
        }

        let Some(records) = self.records.as_mut() else {
            return;
        };
        self.pending = records.next().map(|record| match record {
            Ok(record) => Python::attach(|py| parse_record(py, &self.parse, &record)),
            Err(err) => Err(format!("{}: {err}", self.shown).into()),
        });
    }

    fn schedule_next(&mut self, cx: &mut morel::Ctx) {
        self.ensure_pending();
        match self.pending.as_ref() {
            Some(Ok((at, _))) if *at < cx.now() => cx.fail(format!(
                "replay source item at {at} is behind the run at {}",
                cx.now()
            )),
            Some(Ok((at, _))) => cx.at(*at),
            Some(Err(_)) => {
                if let Some(Err(err)) = self.pending.take() {
                    cx.fail(err);
                }
            }
            None => {}
        }
    }
}

impl morel::Operator for PyCsvReplay {
    fn on_start(&mut self, cx: &mut morel::Ctx) {
        if cx.is_live() {
            cx.fail("replay source used in a live run");
            return;
        }

        if let Err(err) = self.open() {
            cx.fail(err);
            return;
        }
        self.schedule_next(cx);
    }

    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = match self.pending.take() {
            Some(Ok((_time, value))) => value,
            Some(Err(err)) => {
                cx.fail(err);
                return;
            }
            None => return,
        };

        self.schedule_next(cx);
        self.out.set(value);
    }
}

pub(crate) fn replay_from_csv(
    slf: PyRef<'_, PyGraph>,
    path: PathBuf,
    parse: Py<PyAny>,
) -> PyResult<PyStream> {
    slf.ensure_can_add_nodes()?;
    let shown = path.display().to_string();
    let stream = slf.graph().add(|w| PyCsvReplay {
        path,
        shown,
        parse,
        records: None,
        pending: None,
        out: w.output(),
    });
    Ok(PyStream::wrap(stream, Py::from(slf)))
}

pub(crate) fn replay_from_csv_on_graph(
    graph: &morel::Graph,
    owner: impl Into<PyStreamOwner>,
    path: PathBuf,
    parse: Py<PyAny>,
) -> PyResult<PyStream> {
    let shown = path.display().to_string();
    let stream = graph.add(|w| PyCsvReplay {
        path,
        shown,
        parse,
        records: None,
        pending: None,
        out: w.output(),
    });
    Ok(PyStream::wrap(stream, owner))
}

fn parse_record(py: Python<'_>, parse: &Py<PyAny>, record: &::csv::StringRecord) -> CsvItem {
    let row = PyList::new(py, record.iter().collect::<Vec<_>>()).map_err(callback_error)?;
    let result = parse.bind(py).call1((row,)).map_err(callback_error)?;
    let (nanos, value): (u64, Py<PyAny>) = result
        .extract()
        .map_err(|_| type_error("csv parse callback must return (nanos, value)"))?;
    Ok((morel::Time::from_nanos(nanos), PyValue::new(value)))
}
