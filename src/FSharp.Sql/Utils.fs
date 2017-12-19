[<AutoOpen>]
module internal Utils

module Async =
    let singleton = async.Return
    let map f computation = async.Bind (computation, singleton << f)

module Result =
    let map2 f rx ry =
        rx |> Result.bind (fun x ->
        ry |> Result.map (fun y ->
        f x y))
