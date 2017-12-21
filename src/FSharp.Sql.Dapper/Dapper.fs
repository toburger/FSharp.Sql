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
    member self.AsyncQuerySingleOrDefault<'T>(query: string, param: obj, tn, tm) =
        Async.awaitObj (self.QuerySingleOrDefaultAsync<'T>(query, param, tn, Option.toNullable tm))

    member self.AsyncQueryFirstOrDefault<'T>(query: string, param: obj, tn, tm) =
        Async.awaitObj(self.QueryFirstOrDefaultAsync<'T>(query, param, tn, Option.toNullable tm))

    member self.AsyncQuery<'T>(query: string, param: obj, tn, tm) =
        Async.AwaitTask (self.QueryAsync<'T>(query, param, tn, Option.toNullable tm))

    member self.AsyncQueryMultiple(query: string, param: obj, tn, tm) =
        Async.AwaitTask(self.QueryMultipleAsync(query, param, tn, Option.toNullable tm))

let private tryExecute f =
    Sql.tryExecute (fun ctx -> f ctx.Connection ctx.Transaction ctx.CommandTimeout)

let query<'Result> (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQuery<'Result>(query, null, tn, tm))

let queryMultiple (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQueryMultiple(query, null, tn, tm))

let tryQuerySingle<'Result> (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQuerySingleOrDefault<'Result>(query, null, tn, tm))

let tryQueryFirst<'Result> (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQueryFirstOrDefault<'Result>(query, null, tn, tm))

let queryWithParam<'Result> param (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQuery<'Result>(query, param, tn, tm))

let queryMultipleWithParam param (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQueryMultiple(query, param, tn, tm))

let tryQuerySingleWithParam<'Result> param (query: string) =
    tryExecute (fun conn tn tm -> conn.AsyncQuerySingleOrDefault<'Result>(query, param, tn, tm))

let tryQueryFirstWithParam<'Result> param (query: string) =
    tryExecute (fun conn tn tm-> conn.AsyncQueryFirstOrDefault<'Result>(query, param, tn, tm))

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

let tryQuerySingleWithMap<'Result> map query =
    tryQuerySingleWithParam<'Result> (ofMap map) query

let tryQueryFirstWithMap<'Result> map query =
    tryQueryFirstWithParam<'Result> (ofMap map) query
