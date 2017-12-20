# FSharp.Sql

WIP - The idea is to have the ability to compose SQL commands together.  
The execution is postponed till the end. You can compare it with the Async builder in F#, where no async operation is executed until you manually force it by calling `Async.RunSynchronously`.

A (very contrived) example could be the following code:

```fsharp
let program = sql {
    let! count = getUserCount ()
    let! users = getUsers ()
    let! user = tryGetUser 1
    return (count, users, user)
}
````

You can define a set of operations inside the `sql` computation expression.

`getUsers` looks as following, leveraging Dapper to retrieve Users from the Db:

```fsharp
let getUsers () =
    Dapper.query<User> "SELECT id, name FROM users"
```

> As you can see, you can leverage all kinds of database related libraries as long as they expose an `IDbConnection`.

Another workflow could be the setup logic for testing the logic:

```fsharp
let setup data action = sql {
    do! createUsersTable ()
    do! insertData data
    let! res = action
    do! dropUsersTable ()
    return res
}
```

This allows also to compose the commands not only step by step, but you can pass a command also as parameter `action` (which could be the `program` from before).

Finally you execute the program by running the following commands.

```fsharp
program
|> setup data
|> Sql.execute connectionCreator
|> Async.RunSynchronously
|> function
    | Ok result -> // SUCCESS!
    | Error exn -> // ERROR!
```

The fundamental data type is an `SqlAction` which is technically a `Result` wrapped in an `Async` type.