open FSharp.Sql
open System.Data.SqlClient
open System.Data

[<CLIMutable>]
type User =
    { Id: int
      Name: string }

let getUsers () =
    Dapper.query<User> "SELECT id, name FROM users"

let tryGetUser id =
    Dapper.tryQuerySingleWithMap<User>
        (Map ["id", id])
        "SELECT id, name FROM users WHERE id = @id"

let getUserCount () =
    Command.count (Table ("dbo", "users"))

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
    let! count = getUserCount ()
    let! users = getUsers ()
    let! user = tryGetUser 1
    //let! user = tryGetUser -1
    return (count, users, user)
}

[<EntryPoint>]
let main _ =
    program
    |> Sql.execute connectionCreator
    |> Async.RunSynchronously
    |> function
        | Ok (count, users, singleUser) ->
            printfn "Count of users: %i" count
            printfn "------------"
            for user in users do
                printfn "%i: %s" user.Id user.Name
            printfn "------------"
            match singleUser with
            | Some user ->
                printfn "The single user's name is %s" user.Name
            | None ->
                printfn "No single user was found"
        | Error exn -> printfn "FAILURE: %s" exn.Message
    0
