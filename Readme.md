# FSharp.Sql [![Build Status](https://travis-ci.org/toburger/FSharp.Sql.svg?branch=master)](https://travis-ci.org/toburger/FSharp.Sql)

WIP - The idea is to have the ability to compose SQL commands together.  
The execution is postponed till the end. You can compare it with the Async builder in F#, where no async operation is executed until you manually force it by calling `Async.RunSynchronously`.

A (very contrived) example could be the following code:

```fsharp
// SqlAction<int64 * seq<User> * User option>
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
// unit -> SqlAction<seq<User>>
let getUsers () =
    Dapper.query<User> "SELECT id, name FROM users"
```

> As you can see, you can leverage all kinds of database related libraries as long as they expose an `IDbConnection`.

Another workflow could be the setup logic for testing the logic:

```fsharp
// seq<User> -> SqlAction<'a> -> SqlAction<'a>
let setup users action = sql {
    do! createUsersTable ()
    do! insertUsersData users
    let! result = action
    do! dropUsersTable ()
    return result
}
```

This allows us to compose the commands not only step by step, but you can pass a command also as parameter `action` (which could be the `program` from before).

Finally you execute the program by running the following commands.

```fsharp
program
|> setup users
|> Sql.execute connectionCreator
|> Async.RunSynchronously
|> function
    | Ok result -> // SUCCESS!
    | Error exn -> // ERROR!
```

The fundamental data type is an `SqlAction` which is technically a `Result` wrapped in an `Async` type.

## Appendix

If the above `program` code looks way too imperative to you, you could write it in a more monadic style (the `sql` computation expression under the hood does exactly the same):

``` fsharp
let (>>=) a b = Sql.bind b a

let program =
    getUserCount ()
    >>= fun count ->
    getUsers ()
    >>= fun users ->
    tryGetUser 1
    >>= fun user ->
    Sql.ok (count, users, user)
```

Or in an applicative style:

```fsharp
let (<!>) = Sql.map
let (<*>) = Sql.apply
let tuple3 a b c = (a, b, c)

let program =
    tuple3 <!>
        getUserCount () <*>
        getUsers () <*>
        tryGetUser 1
```
