open FSharp.Sql
open System.Data
open Microsoft.Data.Sqlite

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
    Command.count (Table "users")

let connectionString =
    """DataSource=:memory:"""

let connectionCreator () =
    new SqliteConnection(connectionString) :> IDbConnection

let createUsersTable () =
    let fields =
        [ { Name = "id"; Type = Int }
          { Name = "name"; Type = Varchar(50) } ]
    Command.create (Table "users") fields
    |> Sql.map ignore

let insertData data =
    let param name (value: obj) = SqliteParameter(name, value)
    [ for id, name in data ->
        Command.executeNonQueryWith
            [ param "id" (box id)
              param "name" (box name) ]
            "INSERT INTO users (id, name) VALUES (@id, @name)" ]
    // |> SqlExtras.Parallel
    |> SqlExtras.sequence
    |> Sql.map ignore

let dropUsersTable () =
    Command.drop (Table "users")
    |> Sql.map ignore

let setup data action = sql {
    do! createUsersTable ()
    do! insertData data
    let! res = action
    do! dropUsersTable ()
    return res
}

/// Very contrived example, but the idea is to have a composable
/// workflow of SQL actions, that can be combined together
let program = sql {
    let! count = getUserCount ()
    let! users = getUsers ()
    let! user = tryGetUser 1
    //let! user = tryGetUser -1
    return (count, users, user)
}

let data =
    [ 1, "Tobias"
      2, "Lorenz"
      3, "Stefanie" ]

[<EntryPoint>]
let main _ =
    program
    |> setup data
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
