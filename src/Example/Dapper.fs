module Dapper

open System.Collections.Generic
open System.Data
open System.Dynamic
open FSharp.Sql
open Dapper

let private ofObj (x: 'T): 'T option =
    match box x with
    | null -> None
    | obj -> Some (unbox obj)

module private Async =
    let singleton = async.Return
    let map f computation = async.Bind (computation, singleton << f)
    let awaitObj task =
        task
        |> Async.AwaitTask
        |> map ofObj

type IDbConnection with
    member self.AsyncQuerySingle<'T>(query: string) =
        Async.AwaitTask(self.QuerySingleAsync<'T>(query))
    member self.AsyncQuerySingleOrDefault<'T>(query: string) =
        Async.awaitObj (self.QuerySingleOrDefaultAsync<'T>(query))
    member self.AsyncQuerySingleOrDefault<'T>(query: string, param: obj) =
        Async.awaitObj (self.QuerySingleOrDefaultAsync<'T>(query, param))
    member self.AsyncQuerySingle<'T>(query: string, param: obj) =
        Async.AwaitTask(self.QuerySingleAsync<'T>(query, param))

    member self.AsyncQueryFirst<'T>(query: string) =
        Async.AwaitTask(self.QueryFirstAsync<'T>(query))
    member self.AsyncQueryFirst<'T>(query: string, param: obj) =
        Async.AwaitTask(self.QueryFirstAsync<'T>(query, param))
    member self.AsyncQueryFirstOrDefault<'T>(query: string) =
        Async.awaitObj(self.QueryFirstOrDefaultAsync<'T>(query))
    member self.AsyncQueryFirstOrDefault<'T>(query: string, param: obj) =
        Async.awaitObj(self.QueryFirstOrDefaultAsync<'T>(query, param))

    member self.AsyncQuery<'T>(query: string) =
        Async.AwaitTask (self.QueryAsync<'T>(query))
    member self.AsyncQuery<'T>(query: string, param: obj) =
        Async.AwaitTask (self.QueryAsync<'T>(query, param))

    member self.AsyncQueryMultiple(query: string) =
        Async.AwaitTask(self.QueryMultipleAsync(query))
    member self.AsyncQueryMultiple(query: string, param: obj) =
        Async.AwaitTask(self.QueryMultipleAsync(query, param))

let private tryExecute f =
    Sql.tryExecute (fun ctx -> f ctx.Connection)

let query<'Result> (query: string) =
    tryExecute (fun conn -> conn.AsyncQuery<'Result>(query))

let queryMultiple (query: string) =
    tryExecute (fun conn -> conn.AsyncQueryMultiple(query))

let querySingle<'Result> (query: string) =
    tryExecute (fun conn -> conn.AsyncQuerySingle<'Result>(query))

let tryQuerySingle<'Result> (query: string) =
    tryExecute (fun conn -> conn.AsyncQuerySingleOrDefault<'Result>(query))

let queryFirst<'Result> (query: string) =
    tryExecute (fun conn -> conn.AsyncQueryFirst<'Result>(query))

let queryWithParam<'Result> param (query: string) =
    tryExecute (fun conn -> conn.AsyncQuery<'Result>(query, param))

let queryMultipleWithParam param (query: string) =
    tryExecute (fun conn -> conn.AsyncQueryMultiple(query, param))

let querySingleWithParam<'Result> param (query: string) =
    tryExecute (fun conn -> conn.AsyncQuerySingle<'Result>(query, param))

let tryQuerySingleWithParam<'Result> param (query: string) =
    tryExecute (fun conn -> conn.AsyncQuerySingleOrDefault<'Result>(query, param))

let queryFirstWithParam<'Result> param (query: string) =
    tryExecute (fun conn -> conn.AsyncQueryFirst<'Result>(query, param))

let tryQueryFirstWithParam<'Result> param (query: string) =
    tryExecute (fun conn -> conn.AsyncQueryFirstOrDefault<'Result>(query, param))

let private ofMap (map: Map<string, _>) =
    let expando = ExpandoObject()
    let expandoDictionary = expando :> IDictionary<string, obj>
    for paramValue in map do
        expandoDictionary.Add(paramValue.Key, paramValue.Value :> obj)
    expando

let queryWithMap<'Result> map query =
    queryWithParam<'Result> (ofMap map) query

let queryMultipleWithMap map query =
    queryMultipleWithParam (ofMap map) query

let querySingleWithMap<'Result> map query =
    querySingleWithParam<'Result> (ofMap map) query

let tryQuerySingleWithMap<'Result> map query =
    tryQuerySingleWithParam<'Result> (ofMap map) query

let queryFirstWithMap<'Result> map query =
    queryFirstWithParam<'Result> (ofMap map) query

let tryQueryFirstWithMap<'Result> map query =
    tryQueryFirstWithParam<'Result> (ofMap map) query
