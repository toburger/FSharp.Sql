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
    Dapper.queryWithMap
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

[<EntryPoint>]
let main argv =
    getUsers ()
    |> Sql.execute connectionCreator
    |> Async.RunSynchronously
    |> printfn "%A"
    0 // return an integer exit code
