use crate::db;
use crate::Server;
use actix_web::FutureResponse;
use actix_web::{HttpResponse, State, Json, Query};
use actix_web_multipart_file::{Multiparts, FormData};
use diesel::pg::PgConnection;
use failure::Error;
use futures::prelude::*;
use itertools::Itertools;
use std::io::{BufReader, Read};
use log::debug;

/// POST /csvのハンドラ
pub fn handle_post_csv(
    server: State<Server>,
    multiparts: Multiparts,
) -> FutureResponse<HttpResponse> {
    let fut = multiparts
        .from_err()
        .filter(|field| field.content_type == "text/csv")
        .filter_map(|field| match field.form_data {
            FormData::File { file, .. } => Some(file),
            FormData::Data { .. } => None,
        })
        .and_then(move |file| load_file(&*server.pool.get()?, file))
        .fold(0, |acc, x| Ok::<_, Error>(acc + x))
        .map(|sum| HttpResponse::Ok().json(api::csv::post::Response(sum)))
        .from_err();
    Box::new(fut)
}

fn load_file(conn: &PgConnection, file: impl Read) -> Result<usize, Error> {
    use crate::model::NewLog;
    let mut ret = 0;
    let in_csv = BufReader::new(file);
    let in_log = csv::Reader::from_reader(in_csv).into_deserialize::<::api::Log>();
    for logs in &in_log.chunks(1000) {
        let logs = logs
            .filter_map(Result::ok)
            .map(|log| NewLog {
                user_agent: log.user_agent,
                response_time: log.response_time,
                timestamp: log.timestamp.naive_utc(),
            })
            .collect_vec();
        let inserted = db::insert_logs(conn, &logs)?;
        ret += inserted.len();
    }

    Ok(ret)
}

/// POST /logsのハンドラ
pub fn handle_post_logs(
    server: State<Server>,
    log: Json<api::logs::post::Request>,
) -> Result<HttpResponse, Error> {
    use chrono::Utc;
    use crate::model::NewLog;

    let log = NewLog {
        user_agent: log.user_agent.clone(),
        response_time: log.response_time,
        timestamp: log.timestamp.unwrap_or_else(|| Utc::now()).naive_utc(),
    };
    let conn = server.pool.get()?;
    let id = db::insert_log(&conn, &log)?;

    debug!("recieved log: {:?}", log);
    debug!("created id: {:?}", id);

    Ok(HttpResponse::Accepted().finish())
}

/// GET /logsのハンドラ
pub fn handle_get_logs(
    server: State<Server>,
    range: Query<api::logs::get::Query>,
) -> Result<HttpResponse, Error> {
    use chrono::{DateTime, Utc};

    let conn = server.pool.get()?;
    let logs = db::logs(&conn, range.from, range.until)?;
    let logs = logs
        .into_iter()
        .map(|log| api::Log {
            user_agent: log.user_agent,
            response_time: log.response_time,
            timestamp: DateTime::from_utc(log.timestamp, Utc),
        })
        .collect();

    debug!("{:?}", range);
    Ok(HttpResponse::Ok().json(api::logs::get::Response(logs)))
}

/// GET /csvのハンドラ
pub fn handle_get_csv(
    server: State<Server>,
    range: Query<api::csv::get::Query>,
) -> Result<HttpResponse, Error> {
    use chrono::{DateTime, Utc};

    let conn = server.pool.get()?;
    let logs = db::logs(&conn, range.from, range.until)?;
    let v = Vec::new();
    let mut w = csv::Writer::from_writer(v);

    for log in logs.into_iter().map(|log| ::api::Log {
        user_agent: log.user_agent,
        response_time: log.response_time,
        timestamp: DateTime::from_utc(log.timestamp, Utc),
    }) {
        w.serialize(log)?;
    }

    let csv = w.into_inner()?;
    Ok(HttpResponse::Ok()
        .header("Content-Type", "text/csv")
        .body(csv))
}