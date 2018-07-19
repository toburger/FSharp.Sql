[<AutoOpen>]
module internal Utils

module Async =
    let inline map f computation = async.Bind (computation, async.Return << f)

module Result =
    let inline map2 f rx ry =
        rx |> Result.bind (fun x ->
        ry |> Result.map (fun y ->
        f x y))
