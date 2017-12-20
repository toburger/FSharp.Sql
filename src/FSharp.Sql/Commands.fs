namespace FSharp.Sql

[<RequireQualifiedAccess>]
module Command =
    open System.Data
    open Microsoft.FSharp.Reflection

    let (|Table|) (table: Table) = table.GetString()

    let commandWith (parameters: IDbDataParameter list) sql (ctx: SqlContext) =
        let cmd = ctx.Connection.CreateCommand()
        cmd.CommandText <- sql
        cmd.Transaction <- ctx.Transaction
        cmd.CommandTimeout <- ctx.CommandTimeout
        parameters |> List.iter (cmd.Parameters.Add >> ignore)
        cmd

    let executeNonQueryWith parameters sql =
        Sql.tryExecute (fun ctx ->
            use cmd = commandWith parameters sql ctx
            cmd.AsyncExecuteNonQuery())

    let executeNonQuery action =
        executeNonQueryWith [] action

    let executeScalarWith<'T> parameters sql: SqlAction<'T> =
        Sql.tryExecute (fun ctx -> async {
            use cmd = commandWith parameters sql ctx
            let! res = cmd.AsyncExecuteScalar()
            return unbox<'T> res
        })

    let executeScalar<'T> =
        executeScalarWith<'T> []

    let field name typ = { Name = name; Type = typ }

    let createTableQuery (Table table) (fields: Field list) =
        let fields = String.concat ", " (List.map string fields)
        sprintf "CREATE TABLE %s (%s)" table fields

    let create table fields =
        createTableQuery table fields
        |> executeNonQuery

    let count (Table table) =
        sprintf "SELECT COUNT(*) FROM %s" table
        |> executeScalar
        |> Sql.map unbox<int64>

    let createIndex (indexType: IndexType) idxName (Table table) (fields: Field list) =
        let fields = String.concat ", " (List.map (fun f -> sprintf "[%s]" f.Name) fields)
        let indexType =
            match indexType with
            | Clustered -> "CLUSTERED"
            | NonClustered -> "NONCLUSTERED"
        sprintf """CREATE %s INDEX [%s] ON %s (%s)""" indexType idxName table fields
        |> executeNonQuery

    let inline fromDBNull (x: obj) =
        if obj.Equals(x, System.DBNull.Value)
        then Unchecked.defaultof<_>
        else x

    let readAllWith<'T> parameters sql =
        let readAll ctx = seq {
            if not (FSharpType.IsRecord(typeof<'T>)) then
                invalidOp (sprintf "'%s' is not an F# record" typeof<'T>.FullName)
            let init = FSharpValue.PreComputeRecordConstructor(typeof<'T>)
            let fieldCount = FSharpType.GetRecordFields(typeof<'T>).Length
            let cmd = commandWith parameters sql ctx
            use rdr = cmd.ExecuteReader()
            while rdr.Read() do
                let arr = Array.zeroCreate fieldCount
                rdr.GetValues(arr) |> ignore
                for i in 0..arr.Length-1 do
                    arr.[i] <- fromDBNull arr.[i]
                yield unbox<'T> (init arr)
        }
        Sql.tryExecute (Async.singleton << readAll)

    let readAllBy<'T> =
        readAllWith<'T> []

    let getFields<'T> =
        FSharpType.GetRecordFields(typeof<'T>)
        |> Array.map (fun f -> sprintf "[%s]" f.Name)
        |> String.concat ", "

    let readAll<'T> (Table table) =
        sprintf "SELECT %s FROM %s" getFields<'T> table
        |> readAllBy<'T>

    let drop (Table table) =
        sprintf "DROP TABLE %s" table
        |> executeNonQuery

    let rename (Table otable) (Table ntable) =
        sprintf "EXEC sp_rename '%s', '%s'" otable ntable
        |> executeNonQuery
