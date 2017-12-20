module Dapper

open System.Collections.Generic
open FSharp.Sql
open Dapper
open System.Dynamic

let query<'Result> (query: string): SqlAction<_, _, seq<'Result>> =
    Sql.tryExecute (fun ctx -> async {
        let! result =
            ctx.Connection.QueryAsync<'Result>(query)
            |> Async.AwaitTask
        return result
    })

let queryWith<'Result> (param: obj) (query: string) =
    Sql.tryExecute (fun ctx -> async {
        let! result =
            ctx.Connection.QueryAsync<'Result>(query, param)
            |> Async.AwaitTask
        return result
    })

let queryWithMap<'Result> (map: Map<string, _>) query =
    let expando = ExpandoObject()
    let expandoDictionary = expando :> IDictionary<string, obj>
    for paramValue in map do
        expandoDictionary.Add(paramValue.Key, paramValue.Value :> obj)
    queryWith expando query
