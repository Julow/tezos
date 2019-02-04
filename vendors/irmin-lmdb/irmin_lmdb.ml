(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

module Option = struct
  let of_result = function
    | Ok v -> Some v
    | _ -> None

  let value_map ~default ~f = function
    | None -> default
    | Some v -> f v

  let get = function
    | Some v -> v
    | None   -> failwith "no value"

end

module Result = struct
  let map ~f = function
    | Ok v -> Ok (f v)
    | Error err -> Error err
end

open Lwt.Infix

type wtxn = Lmdb.rw Lmdb.txn * Lmdb.db

type t = {
  root: string ;
  readonly: bool;
  mutable db: Lmdb.t ;
  mutable wtxn: wtxn option;
}

let of_result op = function
  | Ok v      -> Lwt.return v
  | Error err -> Fmt.kstrf Lwt.fail_with "%s: %a" op Lmdb.pp_error err

let (|>>) v f =
  match v with
  | Ok v -> f v
  | Error e -> Error e

let get_wtxn db =
  match db.wtxn with
  | Some t -> Ok t
  | None ->
      Lmdb.create_rw_txn db.db |>> fun txn ->
      Lmdb.opendb txn |>> fun ddb ->
      db.wtxn <- Some (txn, ddb);
      Ok (txn, ddb)

let src = Logs.Src.create "irmin.lmdb" ~doc:"Irmin in a Lmdb store"
module Log = (val Logs.src_log src : Logs.LOG)

let int64_of_string s =
  try Ok (Int64.of_string s)
  with Failure _ ->
    Error (`Msg (Printf.sprintf "%s is not the representation of an int64" s))

let bool_of_string s =
  try Ok (bool_of_string s)
  with Failure _ ->
    Error (`Msg (Printf.sprintf "%s is not the representation of a boolean" s))

let int64_converter = int64_of_string, Fmt.uint64
let bool_converter = bool_of_string, Fmt.bool

module Conf = struct

  let root = Irmin.Private.Conf.root
  let mapsize =
    Irmin.Private.Conf.key "mapsize" int64_converter 409_600_000_000L
  let readonly =
    Irmin.Private.Conf.key "readonly" bool_converter false

  type t = {
    root    :  string ;
    mapsize : int64 ;
    readonly: bool ;
    (* TODO *)
    (* ?write_buffer_size:int -> *)
    (* ?max_open_files:int -> *)
    (* ?block_size:int -> *)
    (* ?block_restart_interval:int -> *)
    (* ?cache_size:int *)
  }

  let of_config c =
    let root = Irmin.Private.Conf.get c root in
    let mapsize = Irmin.Private.Conf.get c mapsize in
    let readonly = Irmin.Private.Conf.get c readonly in
    let root = match root with None -> "irmin" | Some root -> root in
    { root ; mapsize ; readonly }

end

let config
    ?(config=Irmin.Private.Conf.empty) ?mapsize ?(readonly=false) file =
  let module C = Irmin.Private.Conf in
  let config = C.add config Conf.root (Some file) in
  let config = C.add config Conf.readonly readonly in
  Option.value_map mapsize ~default:config ~f:(C.add config Conf.mapsize)

let open_db ~root ~mapsize ~readonly =
  if not (Sys.file_exists root) then Unix.mkdir root 0o755;
  let flags = if readonly then [ Lmdb.RdOnly ] else [] in
  let sync_flag =
    match Sys.getenv_opt "TEZOS_CONTEXT_SYNC" with
    | None -> []
    | Some s ->
        match String.lowercase_ascii s with
        | "nosync" -> [ Lmdb.NoSync ]
        | "nometasync" -> [ Lmdb.NoMetaSync ]
        | _ ->
            Printf.eprintf "Unrecognized TEZOS_SYNC option : %s\n\
                            allowed: nosync nometasync" s;
            []
  in
  let flags = sync_flag @ Lmdb.NoRdAhead :: Lmdb.NoTLS :: flags in
  let file_flags = if readonly then 0o444 else 0o644 in
  match Lmdb.opendir ~mapsize ~flags root file_flags with
  | Ok db     -> db
  | Error err ->
      Fmt.failwith "open {%s} %a" (Filename.basename root) Lmdb.pp_error err

let dbs = Hashtbl.create 3

let make conf =
  let { Conf.root ; mapsize ; readonly } = Conf.of_config conf in
  try Hashtbl.find dbs (root, readonly)
  with Not_found ->
    let db = open_db ~root ~mapsize ~readonly in
    let db = {
      db; root; readonly;
      wtxn = None;
    } in
    Hashtbl.add dbs (root, readonly) db;
    db

let close t =
  Hashtbl.remove dbs (t.root, t.readonly);
  Lmdb.closedir t.db

type ('r) reader =
  { f : 'k. 'k Lmdb.txn -> Lmdb.db -> ('r, Lmdb.error) result } [@@unboxed]

let with_read_db db ~f =
  match db.wtxn with
  | None ->
      Lmdb.with_ro_db db.db ~f:f.f
  | Some (txn, ddb) ->
      f.f txn ddb

let get txn db k =
  Result.map ~f:Cstruct.of_bigarray (Lmdb.get txn db k)

let find_bind db k ~f =
  match
    with_read_db db ~f:{ f = fun txn db -> Result.map ~f (get txn db k) }
  with
  | Error KeyNotFound -> Ok None
  | Error err -> Error err
  | Ok v -> Ok v

module Raw = struct

  let mem db k =
    with_read_db db ~f:{ f = fun txn db -> Lmdb.mem txn db k }
    |> of_result "mem"

  let find db key of_ba =
    find_bind db key ~f:(fun v -> Option.of_result (of_ba v))
    |> of_result "find"

  let add_string db k v =
    (get_wtxn db |>> fun (txn, ddb) ->
            let res = Lmdb.put_string txn ddb k v in Fmt.epr "Lmdb.put finished.\n%!" ; res)
    |> of_result "add_string"

  let add_cstruct db k v =
    (get_wtxn db |>> fun (txn, ddb) ->
            let res = Lmdb.put txn ddb k (Cstruct.to_bigarray v) in Fmt.epr "Lmdb.put finished.\n%!" ; res )
    |> of_result "add_ba"

  let add db k v =
    Fmt.epr "Raw.add executed.\n%!" ;
    match v with
    | `String v   -> add_string db k v
    | `Cstruct v  -> add_cstruct db k v

  let remove db k =
    (get_wtxn db |>> fun (txn, ddb) ->
     match Lmdb.del txn ddb k with
     | Ok () | Error Lmdb.KeyNotFound -> Ok ()
     | x -> x)
    |> of_result "remove"

  let commit op db =
    (match db.wtxn with
     | None -> Ok ()
     | Some (t, _ddb) ->
         db.wtxn <- None;
         Lmdb.commit_txn t)
    |> of_result op

  let fsync db =
    Lmdb.sync ~force:true db.db
    |> of_result "fsync"

end

module AO (K: Irmin.Hash.S) (V: Irmin.Contents.S0) (Conv: sig
    val of_key: K.t -> string
    val to_value: Cstruct.t -> (V.t, [`Msg of string]) result
    val of_value: V.t -> [`String of string | `Cstruct of Cstruct.t]
    val digest: V.t -> K.t
  end) = struct

  include Conv

  type nonrec t = t
  type key = K.t
  type value = V.t

  let mem db key =
    Raw.mem db (Conv.of_key key)

  let find db key =
    Raw.find db (Conv.of_key key) @@ fun v ->
    Conv.to_value v

  let find_v db key =
    Raw.find db (Conv.of_key key) @@ fun v ->
    Conv.to_value v |>> fun x ->
    Ok (v, x)

  let add db v =
    let k = Conv.digest v in
    let v = Conv.of_value v in
    Raw.add db (Conv.of_key k) v >|= fun () ->
    k

end

module Irmin_value_store
    (M: Irmin.Metadata.S)
    (H: Irmin.Hash.S)
    (C: Irmin.Contents.S)
    (P: Irmin.Path.S) = struct

  module XContents = struct
    module Val = C
    module Key = H
    include AO (Key) (Val) (struct
        let of_key h   = "contents/" ^ Cstruct.to_string (Key.to_raw h)
        let to_value v = Val.of_string (Cstruct.(to_string v))
        let of_value s = `String (Fmt.to_to_string Val.pp s)
        let digest v   = Key.digest Val.t v
      end)
  end

  module Contents = Irmin.Contents.Store(XContents)

  module XNode = struct
    module Key = H
    module Path = P

    module Val = struct
      module Metadata = M

      type kind = [ `Node | `Contents of M.t ]
      type metadata = M.t
      type entry = { kind : kind; name : string; node : H.t; }
      type t = entry list
      type contents = Contents.key
      type node = Key.t
      type step = Path.step
      type value = [`Node of node | `Contents of contents * metadata ]
      let metadata_t = M.t
      let contents_t = Contents.Key.t
      let node_t = Key.t
      let step_t = Path.step_t

      let entry_t =
        let open Irmin.Type in
        record "Tree.entry"
          (fun kind name node ->
             let kind =
               match kind with
               | None -> `Node
               | Some m -> `Contents m in
             { kind ; name ; node } )
        |+ field "kind" (option M.t) (function
            | { kind = `Node ; _ } -> None
            | { kind = `Contents m ; _ } -> Some m)
        |+ field "name" string (fun { name ; _ } -> name)
        |+ field "node" H.t (fun { node ; _ } -> node)
        |> sealr

      let hash_v2_t =
        let open Irmin_v2_type in
        like ~cli:(H.pp, H.of_string)
          (string_of (`Fixed H.digest_size))
          (fun x -> H.of_raw (Cstruct.of_string x))
          (fun x -> Cstruct.to_string (H.to_raw x))

      let metadata_v2_t =
        Irmin_v2_type.(like unit) (fun _ -> M.default) (fun _ -> ())

      let entry_v2_t =
        let open Irmin_v2_type in
        record "Tree.entry"
          (fun kind name node ->
             let kind =
               match kind with
               | None -> `Node
               | Some m -> `Contents m in
             { kind ; name ; node } )
        |+ field "kind" (option metadata_v2_t) (function
            | { kind = `Node ; _ } -> None
            | { kind = `Contents m ; _ } -> Some m)
        |+ field "name" string (fun { name ; _ } -> name)
        |+ field "node" hash_v2_t (fun { node ; _ } -> node)
        |> sealr

      let value_t =
        let open Irmin.Type in
        variant "Tree.value" (fun node contents -> function
            | `Node n          -> node n
            | `Contents (c, m) -> contents (c, m))
        |~ case1 "node"     node_t     (fun n -> `Node n)
        |~ case1 "contents" (pair contents_t M.t) (fun (c, m) -> `Contents (c, m))
        |> sealv

      let of_step = Fmt.to_to_string P.pp_step

      let to_step str = match P.step_of_string str with
        | Ok x           -> x
        | Error (`Msg e) -> failwith e

      let to_entry kind (name, node) =
        { kind; name = of_step name; node }

      let list t  =
        List.fold_left (fun acc { kind; name; node } ->
            let name = to_step name in
            match kind with
            | `Node       -> (name, `Node node) :: acc
            | `Contents m -> (name, `Contents (node, m)) :: acc
          ) [] t
        |> List.rev

      let find t s =
        let s = of_step s in
        let rec aux = function
          | [] -> None
          | x::xs when x.name <> s -> aux xs
          | { kind; node; _ } :: _ ->
              match kind with
              | `Node     -> Some (`Node node)
              | `Contents m -> Some (`Contents (node, m))
        in
        aux t

      type compare_result = LT | EQ | GT

      module Sort_key: sig
        type t
        val of_entry: entry -> t
        val of_contents: string -> t
        val of_node: string -> t
        val order: t -> t -> compare_result
        val compare: t -> t -> int
      end = struct

        type t =
          | Contents: string -> t
          | Node    : string -> t

        exception Result of int

        let str = function Contents s | Node s -> s

        let compare x y = match x, y with
          | Contents x, Contents y -> String.compare x y
          | _  ->
              let xs = str x and ys = str y in
              let lenx = String.length xs in
              let leny = String.length ys in
              let i = ref 0 in
              try
                while !i < lenx && !i < leny do
                  match
                    Char.compare
                      (String.unsafe_get xs !i) (String.unsafe_get ys !i)
                  with
                  | 0 -> incr i
                  | i -> raise (Result i)
                done;
                let get len s i =
                  if i < len then String.unsafe_get (str s) i
                  else if i = len then match s with
                    | Node _     -> '/'
                    | Contents _ -> '\000'
                  else '\000'
                in
                match Char.compare (get lenx x !i) (get leny y !i) with
                | 0 -> Char.compare (get lenx x (!i + 1)) (get leny y (!i + 1))
                | i -> i
              with Result i ->
                i

        let order a b = match compare a b with
          | 0 -> EQ
          | x when x > 0 -> GT
          | _ -> LT

        let of_contents c = Contents c
        let of_node n = Node n

        let of_entry = function
          | {name = n; kind = `Node; _} -> of_node n
          | {name = n; kind = `Contents _; _} -> of_contents n
      end

      let compare_entries a b =
        Sort_key.(compare (of_entry a) (of_entry b))

      (* the order is always:

         [ ...; foo (content key); ...; foo/ (node key);  ... ]

         So always scan until the 'node' key.
      *)

      let remove t step =
        let step = of_step step in
        let node_key = Sort_key.of_node step in
        let contents_key = Sort_key.of_contents step in
        let return ~acc rest = List.rev_append acc rest in
        let rec aux acc = function
          | []     -> t
          | h :: l ->
              let entry_key = Sort_key.of_entry h in
              if Sort_key.order contents_key entry_key = EQ then
                return ~acc l
              else match Sort_key.order node_key entry_key with
                | GT -> aux (h :: acc) l
                | EQ -> return ~acc l
                | LT -> t
        in
        aux [] t

      let hash_of_v = function
        | `Contents (x, _) | `Node x -> x

      let update t step v =
        let step = of_step step in
        let node_key = Sort_key.of_node step in
        let contents_key = Sort_key.of_contents step in
        let return ~acc rest =
          let kind, node = match v with
            | `Node n          -> `Node, n
            | `Contents (c, m) -> `Contents m, c
          in
          let e = { kind; name = step; node} in
          List.rev_append acc (e :: rest)
        in
        let rec aux acc = function
          | [] -> return ~acc []
          | { node; _ } as h :: l ->
              let entry_key = Sort_key.of_entry h in
              (* Remove any contents entry with the same name. This will always
                 come before the new succ entry. *)
              if Sort_key.order contents_key entry_key = EQ then
                aux acc l
              else match Sort_key.order node_key entry_key with
                | GT -> aux (h :: acc) l
                | LT -> return ~acc (h::l)
                | EQ when Cstruct.equal (H.to_raw (hash_of_v v)) (H.to_raw node) -> t
                | EQ -> return ~acc l
        in
        aux [] t

      let empty = []

      let is_empty = function
        | [] -> true
        | _  -> false

      let v alist =
        let alist = List.map (fun (l, x) ->
            let v k = l, k in
            match x with
            | `Node n             -> to_entry `Node (v n)
            | `Contents (c, m) -> to_entry (`Contents m) (v c)
          ) alist
        in
        List.fast_sort compare_entries alist

      let alist t =
        let mk_n k = `Node k in
        let mk_c k m= `Contents (k, m) in
        List.map (function
            | { kind = `Node; name; node } -> (to_step name, mk_n node)
            | { kind = `Contents m; name; node; _ } ->
                (to_step name, mk_c node m)
          ) t

      module N = Irmin.Private.Node.Make (H)(H)(P)(M)
      let to_n t = N.v (alist t)
      let of_n n = v (N.list n)
      let t = Irmin.Type.like N.t of_n to_n
    end

    let v1_t = Irmin.Type.list Val.entry_t

    type entries = { version: int; entries: Val.entry list }

    let v2_t =
      let open Irmin_v2_type in
      record "entries" (fun v entries -> { version = Char.code v; entries })
      |+ field "version" char (fun t -> Char.chr t.version)
      |+ field "entries" (list Val.entry_v2_t) (fun t -> t.entries)
      |> sealr

    let version v = match Cstruct.get_uint8 v 0 with
      | 0 -> `V1
      | 1 -> `V2
      | n -> Fmt.failwith "Unsuppported node version: %d" n

    include AO (Key) (Val) (struct

        let of_key h = "node/" ^ Cstruct.to_string (H.to_raw h)

        let to_value v = match version v with
          | `V1 -> Irmin.Type.decode_cstruct v1_t v
          | `V2 ->
              match Irmin_v2_type.decode_bin v2_t (Cstruct.to_string v) with
              | Ok t -> Ok t.entries
              | Error _ as e -> e

        let of_value v =
          (* always use v2 encoding to write new values *)
          let c = Irmin_v2_type.encode_bin v2_t { entries = v; version = 1 } in
          `String c

        let digest v =
          (* use v1 encoding for the digest *)
          let v = Irmin.Type.encode_cstruct (Irmin.Type.list Val.entry_t) v in
          H.digest Irmin.Type.cstruct v

      end)

  end

  module Node = Irmin.Private.Node.Store(Contents)(P)(M)(XNode)

  module XCommit = struct
    module Val = struct
      type t = {
        node: H.t ;
        parents: H.t list ;
        info: Irmin.Info.t ;
      }
      type commit = H.t
      type node = H.t

      let commit_t = H.t
      let node_t = H.t

      let v ~info ~node ~parents = { info ; node ; parents }
      let xnode { node; _ } = node
      let node t = xnode t
      let parents { parents; _ } = parents
      let info { info; _ } = info

      module C = Irmin.Private.Commit.Make(H)(H)

      let of_c c = v ~info:(C.info c) ~node:(C.node c) ~parents:(C.parents c)

      let to_c { info ; node ; parents } =
        C.v ~info ~node ~parents

      let t = Irmin.Type.like C.t of_c to_c
    end

    module Key = H

    include AO (Key) (Val) (struct
        let of_key h = "commit/" ^ Cstruct.to_string (H.to_raw h)
        let of_value v = `Cstruct (Irmin.Type.encode_cstruct Val.t v)
        let to_value v = Irmin.Type.decode_cstruct Val.t v
        let digest v =
          let v = Irmin.Type.encode_cstruct Val.t v in
          H.digest Irmin.Type.cstruct v
      end)

    let add db v =
      add db v >>= fun k ->
      Raw.commit "Commit.add" db >|= fun () ->
      k

  end
  module Commit = Irmin.Private.Commit.Store(Node)(XCommit)

end

module type Branch = sig
  include Irmin.Branch.S
  val pp_ref: t Fmt.t
  val of_ref: string -> (t, [`Msg of string]) result
end

module Branch (B: Irmin.Branch.S): Branch with type t = B.t = struct
  open Astring
  include B
  let pp_ref ppf b = Fmt.pf ppf "heads/%a" B.pp b

  let of_ref str = match String.cuts ~sep:"/" str with
    | "heads" :: b -> B.of_string (String.concat ~sep:"/" b)
    | _ -> Error (`Msg (Fmt.strf "%s is not a valid branch" str))
end


module Irmin_branch_store (B: Branch) (H: Irmin.Hash.S) = struct

  module Key = B
  module Val = H

  module W = Irmin.Private.Watch.Make(Key)(Val)
  module L = Irmin.Private.Lock.Make(B)

  type nonrec t = {
    db: t;
    w: W.t;
    l: L.t;
  }

  let watches = Hashtbl.create 10

  type key = Key.t
  type value = Val.t
  type watch = W.watch

  let lmdb_of_branch r = "refs/" ^ Fmt.to_to_string B.pp_ref r
  let hash_of_lmdb v = H.of_raw v
  let lmdb_of_hash r = H.to_raw r

  let mem db r =
    Raw.mem db.db (lmdb_of_branch r)

  let unsafe_find db r =
    Raw.find db.db (lmdb_of_branch r) (fun x -> Ok (hash_of_lmdb x))

  let find db r =
    unsafe_find db r

  let watch_key t key ?init f = W.watch_key t.w key ?init f

  let watch t ?init f = W.watch t.w ?init f
  let unwatch t w = W.unwatch t.w w

  let v db (* ~head *) =
    let w =
      try Hashtbl.find watches db.root
      with Not_found ->
        let w = W.v () in
        (* FIXME: we might want to use a weak table *)
        Hashtbl.add watches db.root w;
        w
    in
    let l = L.v () in
    Lwt.return { db ; w; l }

  let list _ =
    (* CR(samoht): normally this should return the references, but
       Tezos don't use that function, so just skip it. *)
    Lwt.return_nil (* TODO, or not *)

  let set_unsafe t r k =
    let r = lmdb_of_branch r in
    let k = lmdb_of_hash k in
    Raw.add_cstruct t.db r k

  let set t r k =
    Log.debug (fun f -> f "set %a" B.pp r);
    L.with_lock t.l r @@ fun () ->
    set_unsafe t r k >>= fun () ->
    Raw.commit "set" t.db

  let remove_unsafe t r =
    let r = lmdb_of_branch r in
    Raw.remove t.db r

  let remove t r =
    Log.debug (fun f -> f "remove %a" B.pp r);
    L.with_lock t.l r @@ fun () ->
    remove_unsafe t r >>= fun () ->
    Raw.commit "remove" t.db

  let eq_hashes = Irmin.Type.equal H.t

  let test_and_set t r ~test ~set =
    Log.debug (fun f -> f "test_and_set");
    L.with_lock t.l r @@ fun () ->
    find t r >>= fun v ->
    let set () =
      (match set with
       | None   -> remove_unsafe t r
       | Some v -> set_unsafe t r v)
      >>= fun () ->
      Raw.commit "test_and_set" t.db >|= fun () ->
      true
    in
    match test, v with
    | None  , None   -> set ()
    | Some v, Some v'-> if eq_hashes v v' then set () else Lwt.return false
    | __ -> Lwt.return false

end


module Make
    (M: Irmin.Metadata.S)
    (C: Irmin.Contents.S)
    (P: Irmin.Path.S)
    (B: Irmin.Branch.S)
    (H: Irmin.Hash.S) = struct

  module P = struct

    module Branch = Irmin_branch_store(Branch(B))(H)
    include Irmin_value_store(M)(H)(C)(P)
    module Slice = Irmin.Private.Slice.Make(Contents)(Node)(Commit)
    module Sync = struct
      type t = unit
      type commit = H.t
      type branch = B.t
      let fetch _ ?depth:_ ~uri:_  _ = Lwt.return_error `Not_available
      let push _ ?depth:_ ~uri:_ _ = Lwt.return_error `Not_available
      let v _ = Lwt.return_unit
    end

    module Repo = struct
      type nonrec t = {
        config: Irmin.config ;
        db: t ;
        branch: Branch.t ;
      }
      let branch_t t : Branch.t = t.branch
      let contents_t t : Contents.t = t.db
      let node_t t : Node.t = contents_t t, t.db
      let commit_t t : Commit.t = node_t t, t.db

      let v config =
        let db = make config in
        Branch.v db >|= fun branch ->
        { db; branch; config }

    end
  end

  include Irmin.Make_ext(P)

  type stats = {
    mutable promoted_contents: int;
    mutable promoted_nodes  : int;
    mutable promoted_commits: int;
    mutable upgraded_nodes : int;
    mutable width: int;
    mutable depth: int;
  }

  let pp_stats ppf t =
    Fmt.pf ppf "[%d blobs/%d nodes (%d upgrades)/%d commits] depth:%d width:%d"
      t.promoted_contents
      t.promoted_nodes
      t.upgraded_nodes
      t.promoted_commits
      t.depth
      t.width

  let stats () = {
    promoted_contents = 0;
    promoted_nodes = 0;
    promoted_commits = 0;
    upgraded_nodes = 0;
    width = 0;
    depth = 0;
  }

  (* poor-man GC *)
  module Irmin_GC = struct

    let incr_contents s = s.promoted_contents <- s.promoted_contents + 1
    let incr_nodes s = s.promoted_nodes <- s.promoted_nodes + 1
    let incr_upgraded_nodes s = s.upgraded_nodes <- s.upgraded_nodes + 1
    let incr_commits s = s.promoted_commits <- s.promoted_commits + 1
    let update_width s c = s.width <- max s.width (List.length c)
    let update_depth s n = s.depth <- max s.depth n

    external get_64: string -> int -> int64 = "%caml_string_get64u"

    module Tbl = Hashset.Make(struct
        type t = string
        let hash c = Int64.to_int (get_64 c (String.length c - 8))
        let equal x y = String.equal x y
      end)

    module Uniq : sig
      type t = private int

      val generate : unit -> t
      val equal : t -> t -> bool
      val hash : t -> int
      val of_int_exn : int -> t
    end = struct
      type t = int

      let generate =
        let idx = ref (-1) in
        fun () -> incr idx ; !idx

      let equal a b = (compare : int -> int -> int) a b = 0

      let hash x = x
      let of_int_exn x = if x < 0 then assert false ; x
    end

    module TransTbl = Hashtbl.Make(Uniq)

    type t = {
      tbl   : Tbl.t;
      new_db: P.Contents.t;
      old_db: P.Contents.t;
      stats : stats;
      switch: Lwt_switch.t option;
    }

    let close t =
      close t.new_db;
      close t.old_db

    let new_root repo =
      let c = Conf.of_config repo.P.Repo.config in
      c.Conf.root ^ ".1"

    let v ?switch repo =
      let config =
        let root = new_root repo in
        Irmin.Private.Conf.add repo.P.Repo.config Conf.root (Some root)
      in
      let new_db = make config in
      let tbl = Tbl.create 10_123 in
      let stats = stats () in
      { tbl; stats; new_db; old_db = repo.db; switch }

    let of_result computation = function
      | Ok v -> v
      | Error err -> Fmt.failwith "%s: lmdb error %a.\n%!" computation Lmdb.pp_error err

    let raw_find db key of_ba =
      find_bind db key ~f:(fun v -> Option.of_result (of_ba v))
      |> of_result "find"

    let promote_val t k v =
      Raw.add_cstruct t.new_db k v

    let is_node k =
      String.length k > 4
      && k.[0] = 'n'
      && k.[1] = 'o'
      && k.[2] = 'd'
      && k.[3] = 'e'

    let upgrade_node t v = match P.XNode.version v with
      | `V2 -> `Cstruct v
      | `V1 ->
          incr_upgraded_nodes t.stats;
          match P.XNode.to_value v with
          | Ok v -> P.XNode.of_value v
          | Error (`Msg e) ->
              Fmt.failwith "Cannot upgrade node %S: %s\n%!"
                (Cstruct.to_string v) e

    let init_time = lazy (Unix.gettimeofday ())
    let last_time = ref 0.

    let print_message t =
      let current_time = Unix.gettimeofday () in
      if current_time -. !last_time > 5. (* print something every 5s *)
      then (
        last_time := current_time;
        Fmt.epr "GC: %d min elapsed - %a\n%!"
          (int_of_float ((!last_time -. Lazy.force init_time) /. 60.))
          pp_stats t.stats;
      )

    let promote msg t ?old k =
      print_message t;

      (match old with
       | Some _ -> old
       | None   -> raw_find t.old_db k (fun x -> Ok x))
      |> function
      | Some v ->
          if is_node k
          then ( Fmt.epr "> Raw.add %S.\n%!" k ; Raw.add t.new_db k (upgrade_node t v) )
          else ( Fmt.epr "> Promote val %S.\n%!" k ; promote_val t k v )
      | None   ->
          Fmt.epr "promote %s: cannot promote key %S\n%!" msg k ;
          let k = H.of_raw (Cstruct.of_string k) in
          Fmt.failwith "promote %s: cannot promote key %a\n%!" msg H.pp k

    let mem t k = Tbl.mem t.tbl k

    type status =
      | Do_scan (* `Gray *) | Do_promotion (* `Black *)
    and value = { key : H.t; derivation : string;
                  status : status ; depth : int }
    and rd_queue = value Queue.t
    and wr_queue = (int, Bigarray.int_elt) Ke.Rke.Weighted.t
    and 'a protected = { (* mutable *) value : 'a; mutex : Lwt_mutex.t }
    and context =
      { rd : rd_queue protected
      ; wr : wr_queue protected
      ; tbl : string TransTbl.t
      ; more : unit Lwt_condition.t
      ; less : unit Lwt_condition.t
      ; gc : t }

    let rec safe_to_promote context (uniq : Uniq.t) =
      Lwt_mutex.lock context.wr.mutex >>= fun () ->
      match Ke.Rke.Weighted.push context.wr.value (uniq :> int) with
      | None ->
          Lwt_condition.wait ~mutex:context.wr.mutex context.less >>= fun () ->
          Lwt_mutex.unlock context.wr.mutex ;
          safe_to_promote context uniq
      | Some () ->
          Lwt_condition.signal context.more () ;
          Lwt_mutex.unlock context.wr.mutex ;
          Lwt.return ()

    let xnode_find_v db key =
      raw_find db (P.XNode.of_key key) @@ fun v ->
      P.XNode.to_value v |>> fun x ->
      Ok (v, x)

    let scan context value =
      let k' = value.derivation in
      match mem context.gc k' with
      | true -> Lwt.return ()
      | false ->
        Tbl.add context.gc.tbl k' ;
        xnode_find_v context.gc.old_db value.key |> Option.get |> fun (_, v) ->
        let children = P.Node.Val.list v in
        incr_nodes context.gc.stats ;
        update_width context.gc.stats children ;
        update_depth context.gc.stats value.depth ;
        Lwt_list.iter_p (fun (_, c) -> match c with
            | `Contents (k, _) ->
                Lwt_mutex.with_lock context.rd.mutex
                  (fun () ->
                     let k' = P.XContents.of_key k in

                     if not (mem context.gc k')
                     then
                       ( incr_contents context.gc.stats ;
                         let uniq = Uniq.generate () in
                         Tbl.add context.gc.tbl k' ;
                         TransTbl.add context.tbl uniq k' ;
                         safe_to_promote context uniq )
                     else Lwt.return () )
            | `Node k ->
                Lwt_mutex.with_lock context.rd.mutex
                  (fun () ->
                     let k' = P.XNode.of_key k in
                     Queue.push { key=k; derivation= k'; status= Do_scan; depth= value.depth + 1 } context.rd.value ;
                     Lwt.return ()))
          children >>= fun () ->
        Lwt_mutex.with_lock context.rd.mutex
          (fun () -> Queue.push { value with status= Do_promotion } context.rd.value ; Lwt.return ())

    let dispatcher ~thread:_ ~signal context () =
      let rec go () =
        let rec consume_to_next_scan () =
          match Queue.top context.rd.value with
          | { status= Do_promotion; derivation= k'; _ } ->
              ignore @@ Queue.pop context.rd.value ;

              if mem context.gc k'
              then
                ( let uniq = Uniq.generate () in
                  TransTbl.add context.tbl uniq k' ;
                  safe_to_promote context uniq >>= fun () -> consume_to_next_scan () )
              else consume_to_next_scan ()
          | to_scan ->
              ignore @@ Queue.pop context.rd.value ;
              Lwt.return (Some to_scan)
          | exception Queue.Empty -> Lwt.return None in

        Lwt_mutex.with_lock context.rd.mutex consume_to_next_scan >>= function
        | Some value -> scan context value >>= go
        | None ->
            Lwt.wakeup signal () ; Lwt.return ()
      in go ()

    let bootstrap ~thread ~signal context () =
      dispatcher ~thread ~signal context ()

    let rec write_thread ~signal context () =
      Lwt_mutex.lock context.wr.mutex >>= fun () ->
      match Ke.Rke.Weighted.pop context.wr.value with
      | Some (-1) ->
          Fmt.epr "Stop write thread.\n%!" ;
          Lwt.wakeup signal () ;
          Lwt_mutex.unlock context.wr.mutex ;
          Lwt.return ()
      | Some uniq ->
          let k' = TransTbl.find context.tbl (Uniq.of_int_exn uniq) in
          Fmt.epr "Promote %S.\n%!" k';
          TransTbl.remove context.tbl (Uniq.of_int_exn uniq) ;
          Lwt_condition.signal context.less () ;
          Lwt_mutex.unlock context.wr.mutex ;
          promote "node" context.gc k' >>= fun () ->
          Fmt.epr "%S promoted.\n%!" k';
          write_thread ~signal context ()
      | None ->
          Fmt.epr "Wait to write.\n%!";
          Lwt_condition.wait ~mutex:context.wr.mutex context.more >>= fun () ->
          Lwt_mutex.unlock context.wr.mutex ;
          write_thread ~signal context ()

    let rec stop_promotion context =
      Lwt_mutex.lock context.wr.mutex >>= fun () ->
      match Ke.Rke.Weighted.push context.wr.value (-1) with
      | None ->
          Lwt_condition.wait ~mutex:context.wr.mutex context.less >>= fun () ->
          Lwt_mutex.unlock context.wr.mutex ;
          stop_promotion context
      | Some () ->
          Lwt_condition.signal context.more () ;
          Lwt_mutex.unlock context.wr.mutex ;
          Lwt.return ()

    let pass gc roots =
      let make_context_from roots =
        let rd_queue = Queue.create () in
        let wr_queue, _ = Ke.Rke.Weighted.create ~capacity:0x10000 Bigarray.Int in

        let rec go = function
          | [] -> ()
          | (k, k') :: roots ->
              Queue.add { key= k; derivation= k'; status= Do_scan; depth= 0 } rd_queue ;
              go roots in
        go roots; { rd= { value= rd_queue; mutex= Lwt_mutex.create () }
                  ; wr= { value= wr_queue; mutex= Lwt_mutex.create () }
                  ; less = Lwt_condition.create ()
                  ; more = Lwt_condition.create ()
                  ; tbl = TransTbl.create 0x100
                  ; gc } in
      let context = make_context_from roots in
      let scan_and_write_threads () =
        let waiter0, signal0 = Lwt.wait () in
        let waiter1, signal1 = Lwt.wait () in
        let waiter2, signal2 = Lwt.wait () in
        let waiter3, signal3 = Lwt.wait () in
        let waiter_writer, signal_writer = Lwt.wait () in

        let thread0 () = Lwt_preemptive.detach (fun signal -> Lwt.async (bootstrap ~thread:0 ~signal context)) signal0 in
        let thread1 () = Lwt_preemptive.detach (fun signal -> Lwt.async (bootstrap ~thread:1 ~signal context)) signal1 in
        let thread2 () = Lwt_preemptive.detach (fun signal -> Lwt.async (bootstrap ~thread:2 ~signal context)) signal2 in
        let thread3 () = Lwt_preemptive.detach (fun signal -> Lwt.async (bootstrap ~thread:3 ~signal context)) signal3 in

        let thread_to_promote () =
          Lwt_preemptive.detach
            (fun signal -> Lwt.async (write_thread ~signal context))
            signal_writer in
        let thread_to_stop () =
          Lwt_preemptive.detach
            (fun context ->
               Lwt.async @@ fun () ->
               waiter0 >>= fun () ->
               waiter1 >>= fun () ->
               waiter2 >>= fun () ->
               waiter3 >>= fun () ->
               stop_promotion context)
            context in

        let final () =
          Lwt.join [ thread0 ()
                   ; thread1 ()
                   ; thread2 ()
                   ; thread3 ()
                   ; thread_to_promote ()
                   ; thread_to_stop (); ]
          >>= fun () ->
          waiter_writer in

        Fmt.epr "Start to scan root node.\n%!" ;
        final () in

      scan_and_write_threads ()

    let copy_root gc k =
      P.XNode.find_v gc.old_db k >|= Option.get >>= fun (buf, _) ->
      let k' = P.XNode.of_key k in
      if mem gc k'
      then Lwt.return ()
      else
        pass gc [ k, k' ] >>= fun () -> promote "root" gc k' ~old:buf

    let copy_commit gc k =
      Lwt_switch.check gc.switch;
      P.XCommit.find_v gc.old_db k >|= Option.get >>= fun (buf, v) ->
      let k' = P.XCommit.of_key k in
      copy_root gc (P.Commit.Val.node v) >>= fun () ->
      incr_commits gc.stats ;
      promote "commit" gc k' ~old:buf

    let root repo =
      let c = repo.P.Repo.config in
      match Irmin.Private.Conf.get c Conf.root with
      | None   -> "context"
      | Some r -> r

    let pivot ~branches repo t =
      let rename () =
        let old_data = Filename.concat (root repo) "data.mdb" in
        let new_data = Filename.concat (new_root repo) "data.mdb" in
        let old_lock = Filename.concat (root repo) "lock.mdb" in
        let new_lock = Filename.concat (new_root repo) "lock.mdb" in
        Lwt_unix.rename new_data old_data >>= fun () ->
        Lwt_unix.unlink new_lock >>= fun () ->
        Lwt_unix.unlink old_lock
      in

      (* promote the live refs *)
      Lwt_list.iter_p (fun br ->
          let k = P.Branch.lmdb_of_branch br in
          promote "refs" t k )
        branches
      >>= fun () ->

      (* fsync *)
      Raw.commit "pivot" t.new_db >>= fun () ->
      Raw.fsync t.new_db >>= fun () ->

      (* rename *)
      close t;
      rename () >>= fun () ->

      (* re-open the database *)
      P.Repo.v repo.config >|= fun x ->
      repo.db.db <- x.db.db

  end

  let promote_all ~(repo:repo) ?before_pivot ~branches t roots =
    Lwt_list.iteri_s (fun i k ->
        Irmin_GC.copy_commit t k >>= fun () ->
        (* flush to disk regularly to not hold too much data into RAM *)
        if i mod 1000 = 0 then ( Raw.commit "flush roots" t.new_db >>= fun () -> Fmt.epr "Database flushed.\n%!" ; Lwt.return () )
        else Lwt.return ()
      ) roots
    >>= fun () ->
    (match before_pivot with
     | None   -> Lwt.return ()
     | Some t -> t ()
    ) >>= fun () ->
    Irmin_GC.pivot ~branches repo t >|= fun () ->
    t.stats

  let gc ~repo ?before_pivot ?(branches=[]) ?switch roots =
    let t, w = Lwt.task () in
    Lwt_switch.add_hook switch (fun () -> t);
    let gc = Irmin_GC.v ?switch repo in
    Lwt.catch
      (fun ()  -> promote_all ~repo ?before_pivot ~branches gc roots)
      (function
        | Lwt_switch.Off -> Lwt.wakeup w (); Lwt.return gc.stats
        | e -> Lwt.fail e)

end
