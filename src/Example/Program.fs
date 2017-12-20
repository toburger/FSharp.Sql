open FSharp.Sql
open System.Data.SqlClient
open System.Data

[<CLIMutable>]
type User =
    { Id: int
      Name: string }

let getUsers () =
    Dapper.query<User> "SELECT id, name FROM users"

let getUser id =
    Dapper.queryWithMap<User>
        (Map ["id", id])
        "SELECT id, name FROM users WHERE id = @id"

let connectionString =
    """Data Source=(localdb)\MSSQLLocalDB;
       Initial Catalog=FSharp.Sql;
       Integrated Security=True;
       Connect Timeout=30;
       Encrypt=False;
       TrustServerCertificate=True;
       ApplicationIntent=ReadWrite;
       MultiSubnetFailover=False"""

let connectionCreator () =
    new SqlConnection(connectionString) :> IDbConnection

/// Very contrived example, but the idea is to have a composable
/// workflow of SQL actions, that can be combined together
let program = sql {
    let! users = getUsers ()
    let! user =
        users
        |> Seq.tryHead
        |> Option.map (fun user -> getUser user.Id)
        |> Option.defaultValue (Sql.ok Seq.empty)
        |> Sql.map Seq.tryHead
    return (users, user)
}

[<EntryPoint>]
let main _ =
    program
    |> Sql.execute connectionCreator
    |> Async.RunSynchronously
    |> function
        | Ok (users, singleUser) ->
            for user in users do
                printfn "%i: %s" user.Id user.Name
            printfn "------------"
            singleUser
            |> Option.iter (fun user ->
                printfn "The single user's name is %s" user.Name)
        | Error exn -> printfn "FAILURE: %s" exn.Message
    0
