module Dapper

open System.Collections.Generic
open FSharp.Sql
open Dapper
open System.Dynamic

let query<'Result> (query: string): SqlAction<_, _, seq<'Result>> =
    SqlAction (fun ctx -> async {
        try
            let! result =
                ctx.Connection.QueryAsync<'Result>(query)
                |> Async.AwaitTask
            return Ok result
        with exn ->
            return Error exn
    })

let queryWith<'Result> (param: obj) (query: string) =
    SqlAction (fun ctx -> async {
        try
            let! result =
                ctx.Connection.QueryAsync<'Result>(query, param)
                |> Async.AwaitTask
            return Ok result
        with exn ->
            return Error exn
    })

let queryWithMap<'Result> (map: Map<string, _>) query =
    let expando = ExpandoObject()
    let expandoDictionary = expando :> IDictionary<string, obj>
    for paramValue in map do
        expandoDictionary.Add(paramValue.Key, paramValue.Value :> obj)
    queryWith expando query
