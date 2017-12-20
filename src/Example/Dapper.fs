module Dapper

open System.Collections.Generic
open System.Data
open System.Dynamic
open FSharp.Sql
open Dapper

type IDbConnection with
    member self.AsyncQuery<'T>(query: string) =
        Async.AwaitTask (self.QueryAsync<'T>(query))
    member self.AsyncQuery<'T>(query: string, param: obj) =
        Async.AwaitTask (self.QueryAsync<'T>(query, param))

let query<'Result> (query: string) =
    Sql.tryExecute (fun ctx ->
        ctx.Connection.AsyncQuery<'Result>(query))

let queryWith<'Result> (param: obj) (query: string) =
    Sql.tryExecute (fun ctx ->
        ctx.Connection.AsyncQuery<'Result>(query, param))

let queryWithMap<'Result> (map: Map<string, _>) query =
    let expando = ExpandoObject()
    let expandoDictionary = expando :> IDictionary<string, obj>
    for paramValue in map do
        expandoDictionary.Add(paramValue.Key, paramValue.Value :> obj)
    queryWith<'Result> expando query
