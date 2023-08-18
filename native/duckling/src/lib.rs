/* This looks like it's not going to be nearly as straightforward as I'd hoped
Given the requirements of certain DuckDB elements to have certain non-static lifetimes (ex, statements),
as well as the requirements of Rustler ResourceArcs to have static lifetimes, the drop-in version where
the values themselves/references to them are shared between both Rust and Elixir, the way they are with the C++ version,
seems impossible. The best alternitative I can come up with is somehow giving the ResourceArc some data that points to
some data held only in rust, like maybe a hash key or something, where the data in Rust can be modified while avoiding
trying to deal with the requirements of ResourceArc. 



The hashmap idea laid out below doesn't work. It uses global (read: static) variables,
so the data inserted into them still needs to have a static lifespan.
Reading what people have done online, most of the solutions assume a "main" function that starts everything else,
letting you declare a non-static variable there and pass it through to whatever needs it.
This is a library, so it doesn't have a "main". I'm sure there's a way to do this, but I don't know it.

Current structure for connections:
RwLock on a HashMap
HashMap keys are ints, insertion order, value is always unique for up to u64 connections
HashMap values are Mutex-locked Connections

Upsides:
should actually function, significant benefit over ResourceArc solution
can *read* values from many connections at the same time
thread safety is ensured for the connections by wrapping it in a mutex
Downsides:
Only one connection can be modified at any given point in time
If any connections are actively being read, no connections can be modified
Unclear how this interacts with items that are made from connections, ex queries
Only one element may access a given connection in any way at a given point in time
*/

use std::{sync::{Mutex, RwLock}, collections::HashMap};
use duckdb::{Connection, Rows, Statement, Appender, Config, AccessMode, DefaultOrder, DefaultNullOrder};
use rustler::{Atom, Env, Term, Encoder, ResourceArc, Decoder};

#[rustler::nif(schedule = "DirtyIo")]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

rustler::atoms! {
    ok,
    error,
    nil,
    access_mode,
    automatic,
    read_only,
    read_write,
    checkpoint_wal_size,
    use_direct_io,
    load_extensions,
    maximum_memory,
    maximum_threads,
    use_temporary_directory,
    temporary_directory,
    collation,
    default_order_type,
    asc,
    desc,
    default_null_order,
    nulls_firs,
    nulls_last,
    enable_external_access,
    object_cache_enable,
    force_checkpoint,
    checkpoint_on_shutdown,
    force_compression,
    auto,
    uncompressed,
    constant,
    rle,
    dictionary,
    pfor_delta,
    bitpacking,
    fsst,
    chimp,
    patas,
    force_bitpacking_mode,
    constant_delta,
    delta_for,
    for_atom = "for",
    preserve_insertion_order,
    extension_directory,
    allow_unsigned_extensions,
    immediate_transaction_mode,
    memory_allocator,
    duckdb,
    erlang
}

struct RustlerConn<'a, 'b: 'a> {
    connection: &'a Connection,
    queries: HashMap<u64, Mutex<Rows<'b>>>,
    lifetime_queries: u64,
    statements: HashMap<u64, Mutex<Statement<'b>>>,
    lifetime_statements: u64
}
unsafe impl Sync for RustlerConn<'_, '_> {}
unsafe impl Send for RustlerConn<'_, '_> {}

struct Conns<'a, 'b> {connections: HashMap<u64, (Mutex<RustlerConn<'a, 'b>>, u32)>, lifetime_connections: u64}
static CONNECTIONS: RwLock<Option<Conns>> = RwLock::new(None);

struct Qrys<'a> {queries: HashMap<u64, Mutex<Rows<'a>>>, lifetime_queries: u64}
unsafe impl Sync for Qrys<'_> {}
unsafe impl Send for Qrys<'_> {}
static QUERIES: RwLock<Option<Qrys>> = RwLock::new(None);
/*struct RustlerConnection {connection: Mutex<Connection>, thread_count: u32}
unsafe impl Sync for RustlerConnection {}
unsafe impl Send for RustlerConnection {}
struct Stmt<'a> {statement: Mutex<Statement<'a>>}
unsafe impl Sync for Stmt<'_> {}
unsafe impl Send for Stmt<'_> {}
struct QueryResult {result: Mutex<Rows<'static>>}
unsafe impl Sync for QueryResult {}
unsafe impl Send for QueryResult {}
struct Append {append: Appender<'static>}
unsafe impl Sync for Append {}
unsafe impl Send for Append {}*/

pub fn load(env: Env, _term: Term) -> bool {
    let mut test = CONNECTIONS.write().unwrap();
    let _ = test.insert(Conns{connections: HashMap::new(), lifetime_connections: 0});
    let mut test2 = QUERIES.write().unwrap();
    let _ = test2.insert(Qrys{queries: HashMap::new(), lifetime_queries: 0});
    //rustler::resource!(RustlerConnection, env);
    //rustler::resource!(Stmt<'static>, env);
    //rustler::resource!(QueryResult, env);
    //rustler::resource!(Append, env);
    true
}

#[rustler::nif(schedule = "DirtyIo")]
fn open<'a>(env: Env<'a>, path: &str, config_settings: HashMap<Term<'a>, Term<'a>>) -> Term<'a> {
    
    let mut thread_count = 0;
    let mut config = Config::default();
    config = match config_settings.get(&access_mode().encode(env)) {
        Some(val) => match Atom::decode(*val) {
            Ok(decoded) => 
                if decoded == automatic() {
                    Config::access_mode(config, AccessMode::Automatic).unwrap()
                }
                else if decoded == read_only() {
                    Config::access_mode(config, AccessMode::ReadOnly).unwrap()
                }
                else if decoded == read_write() {
                    Config::access_mode(config, AccessMode::ReadWrite).unwrap()
                }
                else {config}

            Err(_) => config
        },
        None => config
    };
    config = match config_settings.get(&maximum_memory().encode(env)) {
        Some(val) => Config::max_memory(config, &(u32::decode(*val).unwrap().to_string() + "b")).unwrap(),
        None => config
    };
    config = match config_settings.get(&maximum_threads().encode(env)) {
        Some(val) => {
            thread_count = u32::decode(*val).unwrap();
            Config::threads(config, u32::decode(*val).unwrap() as i64).unwrap()
        },
        None => config
    };
    config = match config_settings.get(&default_order_type().encode(env)) {
        Some(val) => match Atom::decode(*val) {
            Ok(decoded) => 
                if decoded == asc() {
                    Config::default_order(config, DefaultOrder::Asc).unwrap()
                }
                else if decoded == desc() {
                    Config::default_order(config, DefaultOrder::Desc).unwrap()
                }
                else {config}

            Err(_) => config
        },
        None => config
    };
    config = match config_settings.get(&default_null_order().encode(env)) {
        Some(val) => match Atom::decode(*val) {
            Ok(decoded) => 
                if decoded == nulls_firs() {
                    Config::default_null_order(config, DefaultNullOrder::NullsFirst).unwrap()
                }
                else if decoded == nulls_last() {
                    Config::default_null_order(config, DefaultNullOrder::NullsLast).unwrap()
                }
                else {config}

            Err(_) => config
        },
        None => config
    };
    config = match config_settings.get(&enable_external_access().encode(env)) {
        Some(val) => Config::enable_external_access(config, bool::decode(*val).unwrap()).unwrap(),
        None => config
    };
    config = match config_settings.get(&object_cache_enable().encode(env)) {
        Some(val) => Config::enable_object_cache(config, bool::decode(*val).unwrap()).unwrap(),
        None => config
    };
    config = match config_settings.get(&allow_unsigned_extensions().encode(env)) {
        Some(val) => match bool::decode(*val).unwrap() {
            true => Config::allow_unsigned_extensions(config).unwrap(),
            false => config
        },
        None => config
    };


    let conn = match path{
        ":memory:"=> Connection::open_in_memory_with_flags(config),
        _=>Connection::open_with_flags(path, config)
    };
    match conn {
        Ok(connection) => {
            let mut test = CONNECTIONS.write().unwrap();
            let mut conn_object = &mut test.as_mut().unwrap();
            conn_object.lifetime_connections+= 1;
            conn_object.connections.insert(conn_object.lifetime_connections, (Mutex::new(connection), thread_count));
            (ok(), conn_object.lifetime_connections).encode(env)
        },
        Err(err) => (error(), err.to_string()).encode(env)
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn close(env: Env, conn_id: u64) -> Term {
    //Ownership is still a little weird here, so what I'm doing is removing the entire hashmap entry for the element.
    //This *should* tell the mutex to destroy itself, which should by extension tell the connection to close.
    //If I were able to, the command after .unwrap().connections should look like the following line:
    //.get(&conn_id).unwrap().0.lock().unwrap().close();
    //That would, for sure, close the connection directly. But that transfers ownership, so this method should work better.
    CONNECTIONS.write().unwrap().as_mut().unwrap().connections.remove(&conn_id);
    ok().to_term(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn query<'a>(env: Env<'a>, conn_id: u64, qry: &str, params: Vec<Term>) -> Term<'a> {
    let mut test = CONNECTIONS.read().unwrap();
    let mut conn_object = test.as_ref().unwrap().connections.get(&conn_id).unwrap().0.lock().unwrap();
    match conn_object.prepare(qry) {
        Ok(mut stmt) => match stmt.query([]) {
            Ok(result) => {
                let mut test2 = QUERIES.write().unwrap();
                let mut qry_object = test2.as_mut().unwrap();
                qry_object.lifetime_queries += 1;
                qry_object.queries.insert(qry_object.lifetime_queries, Mutex::new(result));
                (ok(), qry_object.lifetime_queries).encode(env)
            },
            Err(err) => (error(), err.to_string()).encode(env)
        }
        Err(err) => (error(), err.to_string()).encode(env),
    }
}

/*#[rustler::nif(schedule = "DirtyIo")]
fn prepare_statement<'a>(env: Env<'a>, arc_connection: ResourceArc<RustlerConnection>, statement: &str) -> Term<'a> {
    let mut connection: Connection = *arc_connection.connection.lock().unwrap();
    match connection.prepare(statement) {
        Ok(statement) => (ok(), ResourceArc::new(Stmt{statement: &statement})).encode(env),
        Err(err) => (error(), err.to_string()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute_statement<'a>(env: Env<'a>, statement: ResourceArc<Stmt>, params: Vec<Term>) -> Term<'a> {
    let mut stmt: &Statement = statement.statement;
    match stmt.query([]) {
        Ok(result) => (ok(), ResourceArc::new(QueryResult{result: &result})).encode(env),
        Err(err) => (error(), err.to_string()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn get_column_names<'a>(env: Env<'a>, query_result: ResourceArc<QueryResult>) -> Term<'a> {
    let mut query: &Rows = query_result.result;
    match query.as_ref() {
        Some(statement) => statement.column_names().encode(env),
        None => make_tuple(env, &[]).encode(env)
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn fetch_chunk<'a>(env: Env<'a>, query_result: ResourceArc<QueryResult>) -> Term<'a> {
    Term{term: 0, env:env}
}

#[rustler::nif(schedule = "DirtyIo")]
fn fetch_all<'a>(env: Env<'a>, query_result: ResourceArc<QueryResult>) -> Term<'a> {
    Term{term: 0, env:env}
}

#[rustler::nif(schedule = "DirtyIo")]
fn appender(env: Env, arc_connection: ResourceArc<RustlerConnection>, table_name: String) -> Term {
    let mut connection: Connection = *arc_connection.connection.lock().unwrap();
    match connection.appender(&table_name) {
        Ok(append) => (ok(), ResourceArc::new(Append{append: &append})).encode(env),
        Err(err) => (error(), err.to_string()).encode(env),
    }
    
}

#[rustler::nif(schedule = "DirtyIo")]
fn appender_add_row<'a>(env: Env<'a>, appender: ResourceArc<Append>, row: Vec<String>) -> Term<'a> {
    Term{term: 0, env:env}
}

#[rustler::nif(schedule = "DirtyIo")]
fn appender_add_rows<'a>(env: Env<'a>, appender: ResourceArc<Append>, rows: Vec<Vec<String>>) -> Term<'a> {
    Term{term: 0, env:env}
}

#[rustler::nif(schedule = "DirtyIo")]
fn appender_flush<'a>(env: Env<'a>, appender: ResourceArc<Append>) -> Term<'a> {
    let mut append: &Appender = appender.append;
    append.flush();
    ok().encode(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn appender_close<'a>(env: Env<'a>, appender: ResourceArc<Append>) -> Term<'a> {
    let term = appender_flush(env, appender);
    let mut append: &Appender = appender.append;
    drop(append);
    term
}

*/

#[rustler::nif(schedule = "DirtyCpu")]
fn library_version(conn_id: u64) -> String {
    let test = CONNECTIONS.read().unwrap();
    let connection = test.as_ref().unwrap().connections.get(&conn_id).unwrap().0.lock().unwrap();
    match connection.version() {
        Ok(vsn) => vsn,
        Err(_) => "".to_string()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn number_of_threads(conn_id: u64) -> u32 {
    CONNECTIONS.read().unwrap().as_ref().unwrap().connections.get(&conn_id).unwrap().1
}


rustler::init!("Elixir.DatabaseThing.NIF", [add, open, close, library_version, number_of_threads], load=load);