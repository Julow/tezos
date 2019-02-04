(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

open State_logging
open Validation_errors

module Shared = struct
  type 'a t = {
    data: 'a ;
    lock: Lwt_mutex.t ;
  }
  let create data = { data ; lock = Lwt_mutex.create () }
  let use { data ; lock } f =
    Lwt_mutex.with_lock lock (fun () -> f data)
end

type global_state = {
  global_data: global_data Shared.t ;
  protocol_store: Store.Protocol.store Shared.t ;
  main_chain: Chain_id.t ;
  protocol_watcher: Protocol_hash.t Lwt_watcher.input ;
  block_watcher: block Lwt_watcher.input ;
}

and global_data = {
  chains: chain_state Chain_id.Table.t ;
  global_store: Store.t ;
  context_index: Context.index ;
}

and chain_state = {
  (* never take the lock on 'block_store' when holding
     the lock on 'chain_data'. *)
  global_state: global_state ;
  chain_id: Chain_id.t ;
  genesis: genesis ;
  faked_genesis_hash: Block_hash.t ;
  expiration: Time.t option ;
  allow_forked_chain: bool ;
  block_store: Store.Block.store Shared.t ;
  context_index: Context.index Shared.t ;
  block_watcher: block Lwt_watcher.input ;
  chain_data: chain_data_state Shared.t ;
  block_rpc_directories:
    block RPC_directory.t Protocol_hash.Map.t Protocol_hash.Table.t  ;
}

and genesis = {
  time: Time.t ;
  block: Block_hash.t ;
  protocol: Protocol_hash.t ;
}

and chain_data_state = {
  mutable data: chain_data ;
  mutable checkpoint: Block_header.t ;
  chain_data_store: Store.Chain_data.store ;
}

and chain_data = {
  current_head: block ;
  current_mempool: Mempool.t ;
  live_blocks: Block_hash.Set.t ;
  live_operations: Operation_hash.Set.t ;
  test_chain: Chain_id.t option ;
  save_point: Int32.t * Block_hash.t  ;
  caboose: Int32.t * Block_hash.t  ;
}

and block = {
  chain_state: chain_state ;
  hash: Block_hash.t ;
  contents: Store.Block.contents ;
  header: Block_header.t ;
}

and hashed_header = {
  chain_state: chain_state ;
  hash: Block_hash.t ;
  header: Block_header.t ;
}


let read_chain_data { chain_data } f =
  Shared.use chain_data begin fun state ->
    f state.chain_data_store state.data
  end

let update_chain_data { chain_id ; context_index ; chain_data } f =
  Shared.use chain_data begin fun state ->
    f state.chain_data_store state.data >>= fun (data, res) ->
    Lwt_utils.may data
      ~f:begin fun data ->
        state.data <- data ;
        Shared.use context_index begin fun context_index ->
          Context.set_head context_index chain_id
            data.current_head.contents.context
        end >>= fun () ->
        Lwt.return_unit
      end >>= fun () ->
    Lwt.return res
  end

(** The number of predecessors stored per block.
    This value chosen to compute efficiently block locators that
    can cover a chain of 2 months, at 1 block/min, which is ~86K
    blocks at the cost in space of ~72MB.
    |locator| = log2(|chain|/10) -1
*)
let stored_predecessors_size = 12

(**
   Takes a block and populates its predecessors store, under the
   assumption that all its predecessors have their store already
   populated. The precedecessors are distributed along the chain, up
   to the genesis, at a distance from [b] that grows exponentially.
   The store tabulates a function [p] from distances to block_ids such
   that if [p(b,d)=b'] then [b'] is at distance 2^d from [b].
   Example of how previous predecessors are used:
   p(n,0) = n-1
   p(n,1) = n-2  = p(n-1,0)
   p(n,2) = n-4  = p(n-2,1)
   p(n,3) = n-8  = p(n-4,2)
   p(n,4) = n-16 = p(n-8,3)
*)
let store_predecessors (store: Store.Block.store) (b: Block_hash.t) : unit Lwt.t =
  let rec loop pred dist =
    if dist = stored_predecessors_size
    then Lwt.return_unit
    else
      Store.Block.Predecessors.read_opt (store, pred) (dist-1) >>= function
      | None -> Lwt.return_unit (* we reached genesis *)
      | Some p ->
          Store.Block.Predecessors.store (store, b) dist p >>= fun () ->
          loop p (dist+1)
  in
  (* the first predecessor is fetched from the header *)
  Store.Block.Header.read_exn (store, b) >>= fun header ->
  let pred = header.shell.predecessor in
  if Block_hash.equal b pred then
    Lwt.return_unit  (* genesis *)
  else
    Store.Block.Predecessors.store (store,b) 0 pred >>= fun () ->
    loop pred 1

(**
   [predecessor s b d] returns the hash of the block at distance [d] from [b].
   Returns [None] if [d] is greater than the distance of [b] from genesis or
   if [b] is genesis.
   Works in O(log|chain|) if the chain is shorter than 2^[stored_predecessors_size]
   and in O(|chain|) after that.
   @raise Invalid_argument "State.predecessors: negative distance"
*)
let predecessor_n_raw store block_hash distance =
  (* helper functions *)
  (* computes power of 2 w/o floats *)
  let power_of_2 n =
    if n < 0 then invalid_arg "negative argument" else
      let rec loop cnt res =
        if cnt<1 then res
        else loop (cnt-1) (res*2)
      in
      loop n 1
  in
  (* computes the closest power of two smaller than a given
     a number and the rest w/o floats *)
  let closest_power_two_and_rest n =
    if n < 0 then invalid_arg "negative argument" else
      let rec loop cnt n rest =
        if n<=1
        then (cnt,rest)
        else loop (cnt+1) (n/2) (rest + (power_of_2 cnt) * (n mod 2))
      in
      loop 0 n 0
  in
  (* actual predecessor function *)
  if distance < 0 then
    invalid_arg ("State.predecessor: distance < 0 " ^ string_of_int distance)
  else if distance = 0 then
    Lwt.return_some block_hash
  else
    let rec loop block_hash distance =
      if distance = 1
      then Store.Block.Predecessors.read_opt (store, block_hash) 0
      else
        let (power,rest) = closest_power_two_and_rest distance in
        let (power,rest) =
          if power < stored_predecessors_size then (power,rest)
          else
            let power = pred stored_predecessors_size in
            let rest = distance - power_of_2 power in
            (power, rest)
        in
        Store.Block.Predecessors.read_opt (store, block_hash) power >>= function
        | None -> Lwt.return_none (* we reached the genesis *)
        | Some predecessor ->
            if rest = 0
            then (* landed on the requested predecessor *)
              Lwt.return_some predecessor
            else (* need to jump further back *)
              loop predecessor rest
    in
    loop block_hash distance

let predecessor_n ?(below_save_point = false) block_store block_hash distance =
  predecessor_n_raw block_store block_hash distance >>= function
  | None ->
      Lwt.return_none
  | Some predecessor ->
      (* check if this block was pruned by the gc *)
      begin
        if below_save_point then
          Store.Block.Header.known (block_store, predecessor)
        else (* Force read of pruned block *)
          Store.Block.Contents.known (block_store, predecessor)
      end
      >>= function
      | false ->
          Lwt.return_none
      | true ->
          Lwt.return_some predecessor

let compute_locator_from_hash chain_state ?(size = 200) head_hash seed =
  Shared.use chain_state.chain_data begin fun state ->
    Lwt.return state.data.caboose
  end >>= fun (_lvl, caboose) ->
  Shared.use chain_state.block_store begin fun block_store ->
    Store.Block.Header.read_exn (block_store, head_hash) >>= fun header ->
    Block_locator.compute
      ~get_predecessor:(predecessor_n ~below_save_point:true block_store)
      ~caboose
      ~size
      head_hash header seed
  end

let compute_locator chain ?size head seed =
  compute_locator_from_hash chain ?size head.hash seed

type t = global_state

module Locked_block = struct

  let store_genesis store genesis context =
    let shell : Block_header.shell_header = {
      level = 0l ;
      proto_level = 0 ;
      predecessor = genesis.block ; (* genesis' predecessor is genesis *)
      timestamp = genesis.time ;
      fitness = [] ;
      validation_passes = 0 ;
      operations_hash = Operation_list_list_hash.empty ;
      context ;
    } in
    let header : Block_header.t = { shell ; protocol_data = MBytes.create 0 } in
    Store.Block.Header.store  (store, genesis.block) header >>= fun () ->
    Store.Block.Contents.store (store, genesis.block)
      { Store.Block.message = Some "Genesis" ;
        max_operations_ttl = 0 ; context ;
        metadata = MBytes.create 0 ;
        last_allowed_fork_level = 0l ;
      } >>= fun () ->
    Lwt.return header

  (* Will that block be compatible with the current checkpoint. *)
  let acceptable chain_data (header : Block_header.t) =
    let checkpoint = chain_data.checkpoint in
    if checkpoint.shell.level < header.shell.level then
      (* the predecessor is assumed compatible. *)
      Lwt.return_true
    else if checkpoint.shell.level = header.shell.level then
      Lwt.return (Block_header.equal header chain_data.checkpoint)
    else (* header.shell.level < level *)
      (* valid only if the current head is lower than the checkpoint. *)
      let head_level =
        chain_data.data.current_head.header.shell.level in
      Lwt.return (head_level < checkpoint.shell.level)

  (* Is a block still valid for a given checkpoint ? *)
  let is_valid_for_checkpoint
      block_store hash (header : Block_header.t) (checkpoint : Block_header.t) =
    if Compare.Int32.(header.shell.level < checkpoint.shell.level) then
      Lwt.return_true
    else
      predecessor_n block_store hash
        (Int32.to_int @@
         Int32.sub header.shell.level checkpoint.shell.level) >>= function
      | None -> assert false
      | Some predecessor ->
          if Block_hash.equal predecessor (Block_header.hash checkpoint) then
            Lwt.return_true
          else
            Lwt.return_false

end

(* Find the branches that are still valid with a given checkpoint, i.e.
   heads with lower level, or branches that goes through the checkpoint. *)
let locked_valid_heads_for_checkpoint block_store data checkpoint =
  Store.Chain_data.Known_heads.read_all
    data.chain_data_store >>= fun heads ->
  Block_hash.Set.fold
    (fun head acc ->
       let valid_header =
         Store.Block.Header.read_exn
           (block_store, head) >>= fun header ->
         Locked_block.is_valid_for_checkpoint
           block_store head header checkpoint >>= fun valid ->
         Lwt.return (valid, header) in
       acc >>= fun (valid_heads, invalid_heads) ->
       valid_header >>= fun (valid, header) ->
       if valid then
         Lwt.return ((head, header) :: valid_heads, invalid_heads)
       else
         Lwt.return (valid_heads, (head, header) :: invalid_heads))
    heads
    (Lwt.return ([], []))

(* Tag as invalid all blocks in `heads` and their predecessors whose
   level strictly higher to 'level'. *)
let tag_invalid_heads block_store chain_store heads level =
  let rec tag_invalid_head (hash, header) =
    if header.Block_header.shell.level <= level then
      Store.Chain_data.Known_heads.store chain_store hash >>= fun () ->
      Lwt.return_some (hash, header)
    else
      let errors = [ Validation_errors.Checkpoint_error (hash, None) ] in
      Store.Block.Invalid_block.store block_store hash
        { level = header.shell.level ; errors } >>= fun () ->
      Store.Block.Contents.remove (block_store, hash) >>= fun () ->
      Store.Block.Operation_hashes.remove_all (block_store, hash) >>= fun () ->
      Store.Block.Operation_path.remove_all (block_store, hash) >>= fun () ->
      Store.Block.Operations_metadata.remove_all (block_store, hash) >>= fun () ->
      Store.Block.Operations.remove_all (block_store, hash) >>= fun () ->
      Store.Block.Predecessors.remove_all (block_store, hash) >>= fun () ->
      Store.Block.Header.read_opt
        (block_store, header.shell.predecessor) >>= function
      | None ->
          Lwt.return_none
      | Some header ->
          tag_invalid_head (Block_header.hash header, header) in
  Lwt_list.iter_p
    (fun (hash, _header) ->
       Store.Chain_data.Known_heads.remove chain_store hash)
    heads >>= fun () ->
  Lwt_list.filter_map_s tag_invalid_head heads

let prune_block store block_hash =
  let st = (store, block_hash) in
  Store.Block.Contents.remove st >>= fun () ->
  Store.Block.Operation_path.remove_all st >>= fun () ->
  Store.Block.Invalid_block.remove store block_hash >>= fun () ->
  Store.Block.Operations_metadata.remove_all st

let delete_block store block_hash =
  prune_block store block_hash >>= fun () ->
  let st = (store, block_hash) in
  Store.Block.Header.remove st >>= fun () ->
  Store.Block.Operations.remove_all st >>= fun () ->
  Store.Block.Operation_hashes.remove_all st >>= fun () ->
  Store.Block.Predecessors.remove_all st


(* Remove all blocks that are not in the chain. *)
let cut_alternate_heads block_store chain_store heads =
  let rec cut_alternate_head hash header =
    Store.Chain_data.In_main_branch.known (chain_store, hash) >>= fun in_chain ->
    if in_chain then
      Lwt.return_unit
    else
      Store.Block.Header.read_opt
        (block_store, header.Block_header.shell.predecessor) >>= function
      | None ->
          delete_block block_store hash >>= fun () -> (* super archive do not delete blocks *)
          Lwt.return_unit
      | Some header ->
          delete_block block_store hash >>= fun () ->
          cut_alternate_head (Block_header.hash header) header in
  Lwt_list.iter_p
    (fun (hash, header) ->
       Store.Chain_data.Known_heads.remove chain_store hash >>= fun () ->
       cut_alternate_head hash header)
    heads

module Chain = struct

  type nonrec genesis = genesis = {
    time: Time.t ;
    block: Block_hash.t ;
    protocol: Protocol_hash.t ;
  }
  let genesis_encoding =
    let open Data_encoding in
    conv
      (fun { time ; block ; protocol } -> (time, block, protocol))
      (fun (time, block, protocol) -> { time ; block ; protocol })
      (obj3
         (req "timestamp" Time.encoding)
         (req "block" Block_hash.encoding)
         (req "protocol" Protocol_hash.encoding))

  type t = chain_state
  type chain_state = t

  let main { main_chain } = main_chain
  let test chain_state =
    read_chain_data chain_state begin fun _ chain_data ->
      Lwt.return chain_data.test_chain
    end

  let allocate
      ~genesis
      ~faked_genesis_hash
      ~save_point
      ~caboose
      ~expiration
      ~allow_forked_chain
      ~current_head
      ~checkpoint
      global_state context_index chain_data_store block_store =
    Store.Block.Contents.read_exn
      (block_store, current_head) >>= fun current_block ->
    Store.Block.Header.read_exn
      (block_store, current_head) >>= fun current_block_head ->
    let rec chain_data = {
      data = {
        save_point ;
        caboose ;
        current_head = {
          chain_state ;
          hash = current_head ;
          contents = current_block ;
          header = current_block_head ;
        } ;
        current_mempool = Mempool.empty ;
        live_blocks = Block_hash.Set.singleton genesis.block ;
        live_operations = Operation_hash.Set.empty ;
        test_chain = None ;
      } ;
      checkpoint ;
      chain_data_store ;
    }
    and chain_state = {
      global_state ;
      chain_id = Chain_id.of_block_hash genesis.block ;
      chain_data = { Shared.data = chain_data ; lock = Lwt_mutex.create () } ;
      genesis ; faked_genesis_hash ;
      expiration ;
      allow_forked_chain ;
      block_store = Shared.create block_store ;
      context_index = Shared.create context_index ;
      block_watcher = Lwt_watcher.create_input () ;
      block_rpc_directories = Protocol_hash.Table.create 7 ;
    } in
    Lwt.return chain_state

  let locked_create
      global_state data ?expiration ?(allow_forked_chain = false)
      chain_id genesis commit =
    let chain_store = Store.Chain.get data.global_store chain_id in
    let block_store = Store.Block.get chain_store
    and chain_data_store = Store.Chain_data.get chain_store in
    let save_point = 0l, genesis.block in
    let caboose = 0l, genesis.block in
    Store.Chain.Genesis_hash.store chain_store genesis.block >>= fun () ->
    Store.Chain.Genesis_time.store chain_store genesis.time >>= fun () ->
    Store.Chain.Genesis_protocol.store chain_store genesis.protocol >>= fun () ->
    Store.Chain_data.Current_head.store chain_data_store genesis.block >>= fun () ->
    Store.Chain_data.Known_heads.store chain_data_store genesis.block >>= fun () ->
    Store.Chain_data.Save_point.store chain_data_store save_point >>= fun () ->
    Store.Chain_data.Caboose.store chain_data_store caboose >>= fun () ->
    begin
      match expiration with
      | None -> Lwt.return_unit
      | Some time -> Store.Chain.Expiration.store chain_store time
    end >>= fun () ->
    begin
      if allow_forked_chain then
        Store.Chain.Allow_forked_chain.store data.global_store chain_id
      else
        Lwt.return_unit
    end >>= fun () ->
    Locked_block.store_genesis
      block_store genesis commit >>= fun genesis_header ->
    Store.Chain_data.Checkpoint.store chain_data_store genesis_header >>= fun () ->
    allocate
      ~genesis
      ~faked_genesis_hash:(Block_header.hash genesis_header)
      ~current_head:genesis.block
      ~expiration
      ~allow_forked_chain
      ~checkpoint:genesis_header
      ~save_point
      ~caboose
      global_state
      data.context_index
      chain_data_store
      block_store

  let create state ?allow_forked_chain genesis  =
    let chain_id = Chain_id.of_block_hash genesis.block in
    Shared.use state.global_data begin fun data ->
      if Chain_id.Table.mem data.chains chain_id then
        Pervasives.failwith "State.Chain.create"
      else
        Context.commit_genesis
          data.context_index
          ~chain_id
          ~time:genesis.time
          ~protocol:genesis.protocol >>= fun commit ->
        locked_create
          state data ?allow_forked_chain chain_id genesis commit >>= fun chain ->
        Chain_id.Table.add data.chains chain_id chain ;
        Lwt.return chain
    end

  let locked_read global_state data id =
    let chain_store = Store.Chain.get data.global_store id in
    let block_store = Store.Block.get chain_store
    and chain_data_store = Store.Chain_data.get chain_store in
    Store.Chain.Genesis_hash.read chain_store >>=? fun genesis_hash ->
    Store.Chain.Genesis_time.read chain_store >>=? fun time ->
    Store.Chain.Genesis_protocol.read chain_store >>=? fun protocol ->
    Store.Chain.Expiration.read_opt chain_store >>= fun expiration ->
    Store.Chain.Allow_forked_chain.known
      data.global_store id >>= fun allow_forked_chain ->
    Store.Block.Header.read (block_store, genesis_hash) >>=? fun genesis_header ->
    let genesis = { time ; protocol ; block = genesis_hash } in
    Store.Chain_data.Current_head.read chain_data_store >>=? fun current_head ->
    Store.Chain_data.Checkpoint.read chain_data_store >>=? fun checkpoint ->
    Store.Chain_data.Save_point.read chain_data_store >>=? fun save_point ->
    Store.Chain_data.Caboose.read chain_data_store >>=? fun caboose ->
    begin
      match expiration with
      | None -> Lwt.return_unit
      | Some time -> Store.Chain.Expiration.store chain_store time
    end >>= fun () ->

    try
      allocate
        ~genesis
        ~faked_genesis_hash:(Block_header.hash genesis_header)
        ~current_head
        ~expiration
        ~allow_forked_chain
        ~checkpoint
        ~save_point
        ~caboose
        global_state
        data.context_index
        chain_data_store
        block_store >>= return
    with Not_found ->
      fail Bad_data_dir

  let locked_read_all global_state data =
    Store.Chain.list data.global_store >>= fun ids ->
    iter_p
      (fun id ->
         locked_read global_state data id >>=? fun chain ->
         Chain_id.Table.add data.chains id chain ;
         return_unit)
      ids

  let read_all state =
    Shared.use state.global_data begin fun data ->
      locked_read_all state data
    end

  let get_exn state id =
    Shared.use state.global_data begin fun data ->
      Lwt.return (Chain_id.Table.find data.chains id)
    end

  let get state id =
    Lwt.catch
      (fun () -> get_exn state id >>= return)
      (function
        | Not_found -> fail (Unknown_chain id)
        | exn -> Lwt.fail exn)

  let all state =
    Shared.use state.global_data begin fun { chains } ->
      Lwt.return @@
      Chain_id.Table.fold (fun _ chain acc -> chain :: acc) chains []
    end

  let id { chain_id } = chain_id
  let genesis { genesis } = genesis
  let faked_genesis_hash { faked_genesis_hash } = faked_genesis_hash
  let expiration { expiration } = expiration
  let allow_forked_chain { allow_forked_chain } = allow_forked_chain
  let global_state { global_state } = global_state
  let checkpoint chain_state =
    Shared.use chain_state.chain_data begin fun { checkpoint } ->
      Lwt.return checkpoint
    end
  let save_point chain_state =
    Shared.use chain_state.chain_data begin fun state ->
      Lwt.return state.data.save_point
    end
  let caboose chain_state =
    Shared.use chain_state.chain_data begin fun state ->
      Lwt.return state.data.caboose
    end

  let purge_loop_full global_store store block_hash bottom =
    let do_prune blocks =
      Store.with_atomic_rw global_store @@ fun () ->
      Lwt_list.iter_s (prune_block store) blocks in
    let rec loop block_hash (n_blocks, blocks) =
      begin if n_blocks >= 4000 then
          do_prune blocks >>= fun () ->
          Lwt.return (0, [])
        else Lwt.return (n_blocks, blocks)
      end >>= fun (n_blocks, blocks) ->
      Store.Block.Header.read_opt (store, block_hash) >>= function
      | None -> assert false (* Should not happen *)
      | Some header ->
          if header.shell.level = bottom then
            do_prune (block_hash :: blocks)
          else
            loop header.shell.predecessor (n_blocks + 1, block_hash :: blocks) in
    Store.Block.Header.read_exn (store, block_hash) >>= fun header ->
    loop header.shell.predecessor (0, [])

  let purge_full chain_state (lvl, hash) =
    Shared.use chain_state.global_state.global_data begin fun global_data ->
      Shared.use chain_state.block_store begin fun store ->
        update_chain_data chain_state begin fun _ data ->
          purge_loop_full global_data.global_store store hash (fst data.save_point) >>= fun () ->
          let new_data = { data with save_point = (lvl, hash) ; } in
          Lwt.return (Some new_data, ())
        end >>= fun () ->
        Shared.use chain_state.chain_data begin fun data ->
          Store.Chain_data.Save_point.store data.chain_data_store (lvl, hash)
        end
      end
    end

  let purge_loop_rolling global_store store ~genesis_hash block_hash limit =
    assert (limit > 0);
    let do_delete blocks =
      Store.with_atomic_rw global_store @@ fun () ->
      Lwt_list.iter_s (delete_block store) blocks in
    let rec prune_loop block_hash limit =
      if limit = 1 then
        Store.Block.Header.read_opt (store, block_hash) >>= function
        | None -> assert false (* Should not happen. *)
        | Some header ->
            prune_block store block_hash >>= fun () ->
            delete_loop header.shell.predecessor (0, []) >>= fun () ->
            Lwt.return block_hash
      else
        Store.Block.Header.read_opt (store, block_hash) >>= function
        | None -> assert false (* Should not happen. *)
        | Some header ->
            prune_block store block_hash >>= fun () ->
            prune_loop header.shell.predecessor (pred limit)
    and delete_loop block_hash (n_blocks, blocks) =
      begin if n_blocks >= 4000 then
          do_delete blocks >>= fun () ->
          Lwt.return (0, [])
        else Lwt.return (n_blocks, blocks)
      end >>= fun (n_blocks, blocks) ->
      Store.Block.Header.read_opt (store, block_hash) >>= function
      | None -> do_delete blocks
      | Some header ->
          if Block_hash.equal genesis_hash block_hash
          then do_delete blocks
          else begin
            delete_loop header.shell.predecessor (n_blocks + 1, block_hash :: blocks)
          end
    in
    Store.Block.Header.read_exn (store, block_hash) >>= fun header ->
    prune_loop header.shell.predecessor limit

  let purge_rolling chain_state ((lvl, hash) as checkpoint) =
    Shared.use chain_state.global_state.global_data begin fun global_data ->
      Shared.use chain_state.block_store begin fun store ->
        Store.Block.Contents.read_exn (store, hash) >>= fun contents ->
        Store.Block.Header.read_exn (store, hash) >>= fun header ->
        let max_op_ttl = contents.max_operations_ttl in
        assert (max_op_ttl > 0);
        let limit = min max_op_ttl (Int32.to_int header.shell.level) in
        purge_loop_rolling ~genesis_hash:chain_state.genesis.block global_data.global_store store hash limit >>= fun caboose_hash ->
        let caboose_level = Int32.sub lvl (Int32.of_int max_op_ttl) in
        let caboose = (caboose_level, caboose_hash) in
        update_chain_data chain_state begin fun _ data ->
          let new_data = { data with save_point = checkpoint ; caboose ; } in
          Lwt.return (Some new_data, ())
        end >>= fun () ->
        Shared.use chain_state.chain_data begin fun data ->
          Store.Chain_data.Save_point.store data.chain_data_store checkpoint >>= fun () ->
          Store.Chain_data.Caboose.store data.chain_data_store caboose >>= fun () ->
          Lwt.return_unit
        end

      end
    end

  let set_checkpoint chain_state checkpoint =
    Shared.use chain_state.block_store begin fun store ->
      Shared.use chain_state.chain_data begin fun data ->
        let head_header =
          data.data.current_head.header in
        let head_hash = data.data.current_head.hash in
        Locked_block.is_valid_for_checkpoint
          store head_hash head_header checkpoint >>= fun valid ->
        assert valid ;
        (* Remove outdated invalid blocks. *)
        Store.Block.Invalid_block.iter store ~f: begin fun hash iblock ->
          if iblock.level <= checkpoint.shell.level then
            Store.Block.Invalid_block.remove store hash
          else
            Lwt.return_unit
        end >>= fun () ->
        (* Remove outdated heads and tag invalid branches. *)
        begin
          locked_valid_heads_for_checkpoint
            store data checkpoint >>= fun (valid_heads, invalid_heads) ->
          tag_invalid_heads store data.chain_data_store
            invalid_heads checkpoint.shell.level >>= fun outdated_invalid_heads ->
          if head_header.shell.level < checkpoint.shell.level then
            Lwt.return_unit
          else
            let outdated_valid_heads =
              List.filter
                (fun (hash, { Block_header.shell } ) ->
                   shell.level <= checkpoint.shell.level &&
                   not (Block_hash.equal hash head_hash))
                valid_heads in
            cut_alternate_heads store data.chain_data_store
              outdated_valid_heads >>= fun () ->
            cut_alternate_heads store data.chain_data_store
              outdated_invalid_heads
        end >>= fun () ->
        (* Store the new checkpoint. *)
        Store.Chain_data.Checkpoint.store
          data.chain_data_store checkpoint >>= fun () ->
        data.checkpoint <- checkpoint ;
        (* TODO 'git fsck' in the context. *)
        Lwt.return_unit
      end
    end

  let set_checkpoint_then_purge_full chain_state checkpoint =
    set_checkpoint chain_state checkpoint >>= fun () ->
    let lvl = checkpoint.shell.level in
    let hash = Block_header.hash checkpoint in
    purge_full chain_state (lvl, hash) >>= fun () ->
    Lwt.return_unit

  let set_checkpoint_then_purge_rolling chain_state checkpoint =
    set_checkpoint chain_state checkpoint >>= fun () ->
    let lvl = checkpoint.shell.level in
    let hash = Block_header.hash checkpoint in
    purge_rolling chain_state (lvl, hash) >>= fun () ->
    Lwt.return_unit

  let acceptable_block chain_state (header : Block_header.t) =
    Shared.use chain_state.chain_data begin fun chain_data ->
      Locked_block.acceptable chain_data header
    end

  let destroy state chain =
    lwt_debug Tag.DSL.(fun f ->
        f "destroy %a"
        -% t event "destroy"
        -% a chain_id (id chain)) >>= fun () ->
    Shared.use state.global_data begin fun { global_store ; chains } ->
      Chain_id.Table.remove chains (id chain) ;
      Store.Chain.destroy global_store (id chain) >>= fun () ->
      Lwt.return_unit
    end

end

type error += Missing_block of Block_hash.t

let () = register_error_kind `Permanent
    ~id:"state.block.missing"
    ~title:"Missing block"
    ~description:"Missing block while looking for predecessor."
    ~pp:(fun ppf block_hash ->
        Format.fprintf ppf
          "@[Cannot find block %a]"
          Block_hash.pp block_hash)
    Data_encoding.(obj1 (req "missing_block" @@ Block_hash.encoding ) )
    (function Missing_block block_header -> Some block_header
            | _ -> None)
    (fun block_header -> Missing_block block_header)

module Block = struct

  type t = block = {
    chain_state: Chain.t ;
    hash: Block_hash.t ;
    contents: Store.Block.contents ;
    header: Block_header.t ;
  }
  type block = t

  type validation_store = {
    context_hash: Context_hash.t;
    message: string option;
    max_operations_ttl: int;
    last_allowed_fork_level: Int32.t;
  }

  module Header = struct

    type t = hashed_header = {
      chain_state: chain_state ;
      hash: Block_hash.t ;
      header: Block_header.t ;
    }
    type block_header = t

    let compare b1 b2 = Block_hash.compare b1.hash b2.hash
    let equal b1 b2 = Block_hash.equal b1.hash b2.hash

    let hash { hash } = hash
    let header { header } = header
    let shell_header { header = { Block_header.shell } } = shell
    let timestamp b = (shell_header b).timestamp
    let fitness b = (shell_header b).fitness
    let level b = (shell_header b).level
    let validation_passes b = (shell_header b).validation_passes

    let known chain_state hash =
      Shared.use chain_state.block_store begin fun store ->
        Store.Block.Header.known (store, hash)
      end

    let read chain_state hash =
      Shared.use chain_state.block_store begin fun store ->
        Store.Block.Header.read (store, hash) >>=? fun header ->
        return { chain_state ; hash ; header }
      end
    let read_opt chain_state hash =
      Shared.use chain_state.block_store begin fun store ->
        Store.Block.Header.read_opt (store, hash) >>= function
        | None -> Lwt.return None
        | Some header -> Lwt.return (Some { chain_state ; header ; hash })
      end
    let read_exn chain_state hash =
      Shared.use chain_state.block_store begin fun store ->
        (Store.Block.Header.read_exn (store, hash) >>= fun header ->
         Lwt.return { chain_state ; header ; hash })
      end

    let of_block ( { chain_state ; hash ; header } : block ) : t =
      { chain_state ; header ; hash }
    let to_block ( { chain_state ; hash ; header } : t ) : block option Lwt.t =
      Shared.use chain_state.block_store begin fun store ->
        Store.Block.Contents.read_opt (store, hash) >>= function
        | Some contents -> Lwt.return_some { chain_state ; hash ; contents ; header }
        | None -> Lwt.return_none
      end

    let all_operation_hashes { chain_state ; hash ; header } =
      Shared.use chain_state.block_store begin fun store ->
        Lwt_list.map_p
          (Store.Block.Operation_hashes.read_exn (store, hash))
          (0 -- (header.Block_header.shell.validation_passes - 1))
      end

    let predecessor { chain_state ; hash ; header } =
      if Block_hash.equal hash header.Block_header.shell.predecessor then
        Lwt.return_none           (* we are at genesis *)
      else
        read_opt chain_state header.Block_header.shell.predecessor

    let predecessor_n chain_state hash n =
      Shared.use chain_state.block_store begin fun block_store ->
        predecessor_n block_store hash n
      end
  end

  let compare b1 b2 = Block_hash.compare b1.hash b2.hash
  let equal b1 b2 = Block_hash.equal b1.hash b2.hash

  let hash { hash } = hash
  let header { header } = header
  let metadata { contents = { metadata } } = metadata
  let chain_state { chain_state } = chain_state
  let chain_id { chain_state = { chain_id } } = chain_id
  let shell_header { header = { shell } } = shell
  let timestamp b = (shell_header b).timestamp
  let fitness b = (shell_header b).fitness
  let level b = (shell_header b).level
  let proto_level b = (shell_header b).proto_level
  let validation_passes b = (shell_header b).validation_passes
  let message { contents = { message } } = message
  let max_operations_ttl { contents = { max_operations_ttl } } =
    max_operations_ttl
  let last_allowed_fork_level { contents = { last_allowed_fork_level } } =
    last_allowed_fork_level

  let is_genesis b = Block_hash.equal b.hash b.chain_state.genesis.block

  let known_valid chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Contents.known (store, hash)
    end
  let known_invalid chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Invalid_block.known store hash
    end
  let read_invalid chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Invalid_block.read_opt store hash
    end
  let list_invalid chain_state =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Invalid_block.fold store ~init:[]
        ~f:(fun hash { level ; errors } acc ->
            Lwt.return ((hash, level, errors) :: acc))
    end
  let unmark_invalid chain_state block =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Invalid_block.known store block >>= fun mem ->
      if mem
      then Store.Block.Invalid_block.remove store block >>= return
      else fail (Block_not_invalid block)
    end

  let is_valid_for_checkpoint block checkpoint =
    let chain_state = block.chain_state in
    Shared.use chain_state.block_store begin fun store ->
      Locked_block.is_valid_for_checkpoint
        store block.hash block.header checkpoint
    end

  let known chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Contents.known (store, hash) >>= fun known ->
      if known then
        Lwt.return_true
      else
        Store.Block.Invalid_block.known store hash
    end

  type partial =
    | Full of block
    | Header of {
        chain_id: Chain_id.t ;
        hash: Block_hash.t ;
        header: Block_header.t
      }
    | Pruned

  let read_predecessor chain_state ~pred ?(below_save_point = false) hash =
    Shared.use chain_state.block_store begin fun store ->
      predecessor_n ~below_save_point store hash pred >>= fun hash_opt ->
      let new_hash_opt =
        match hash_opt with
        | Some _ as hash_opt -> hash_opt
        | None ->
            if Block_hash.equal hash chain_state.genesis.block then
              Some chain_state.genesis.block
            else
              None
      in
      match new_hash_opt with
      | None -> Lwt.fail Not_found
      | Some hash ->
          Store.Block.Contents.read_opt (store, hash) >>= fun contents ->
          Store.Block.Header.read_opt (store, hash) >>= fun header ->
          begin match (contents, header) with
            | (Some contents, Some header) ->
                Lwt.return (Full { chain_state ; hash ; contents ; header })
            | (None, Some header) ->
                let chain_id = chain_state.chain_id in
                Lwt.return (Header { chain_id ; hash ; header })
            | (_, None) ->
                Lwt.return Pruned
          end

    end

  let read chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Contents.read (store, hash) >>=? fun contents -> (* here *)
      Store.Block.Header.read (store, hash) >>=? fun header ->
      return { chain_state ; hash ; contents ; header }
    end

  let read_opt chain_state hash =
    read chain_state hash >>= function
    | Error _ -> Lwt.return_none
    | Ok v -> Lwt.return_some v

  let read_exn chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Header.read_exn (store, hash) >>= fun header ->
      Store.Block.Contents.read_exn (store, hash) >>= fun contents ->
      Lwt.return { chain_state ; hash ; contents ; header }
    end

  (* Quick accessor to be optimized ?? *)

  let predecessor { chain_state ; header ; hash } =
    if Block_hash.equal hash header.shell.predecessor then
      Lwt.return_none           (* we are at genesis *)
    else
      read_opt chain_state header.shell.predecessor

  let predecessor_n b n =
    Shared.use b.chain_state.block_store begin fun block_store ->
      predecessor_n block_store b.hash n
    end

  let store
      ?(dont_enforce_context_hash = false)
      chain_state block_header block_header_metadata
      operations operations_metadata
      { context_hash ; message ; max_operations_ttl ; last_allowed_fork_level } =
    let bytes = Block_header.to_bytes block_header in
    let hash = Block_header.hash_raw bytes in
    fail_unless
      (block_header.shell.validation_passes = List.length operations)
      (failure "State.Block.store: invalid operations length") >>=? fun () ->
    fail_unless
      (block_header.shell.validation_passes = List.length operations_metadata)
      (failure "State.Block.store: invalid operations_data length") >>=? fun () ->
    fail_unless
      (List.for_all2
         (fun l1 l2 -> List.length l1 = List.length l2)
         operations operations_metadata)
      (failure "State.Block.store: inconsistent operations and operations_data") >>=? fun () ->
    (* let's the validator check the consistency... of fitness, level, ... *)
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Invalid_block.known store hash >>= fun known_invalid ->
      fail_when known_invalid (failure "Known invalid") >>=? fun () ->
      Store.Block.Contents.known (store, hash) >>= fun known ->
      if known then
        return_none
      else begin
        (* safety check: never ever commit a block that is not compatible
           with the current checkpoint.  *)
        begin
          let predecessor = block_header.shell.predecessor in
          Store.Block.Header.known
            (store, predecessor) >>= fun valid_predecessor ->
          if not valid_predecessor then
            Lwt.return_false
          else
            Shared.use chain_state.chain_data begin fun chain_data ->
              Locked_block.acceptable chain_data block_header
            end
        end >>= fun acceptable_block ->
        fail_unless
          acceptable_block
          (Checkpoint_error (hash, None)) >>=? fun () ->
        let commit = context_hash in
        Context.exists chain_state.context_index.data commit
        >>= fun exists ->
        fail_unless exists
          (failure "State.Block.store: context hash not found in context")
        >>=? fun _ ->
        fail_unless
          (dont_enforce_context_hash
           || Context_hash.equal block_header.shell.context commit)
          (Inconsistent_hash (commit, block_header.shell.context)) >>=? fun () ->
        let header =
          if dont_enforce_context_hash then
            { block_header
              with shell = { block_header.shell with context = commit } }
          else
            block_header
        in
        let contents = {
          Store.Block.message ;
          max_operations_ttl ;
          last_allowed_fork_level ;
          context = commit ;
          metadata = block_header_metadata ;
        } in
        Store.Block.Header.store (store, hash) header >>= fun () ->
        Store.Block.Contents.store (store, hash) contents >>= fun () ->
        let hashes = List.map (List.map Operation.hash) operations in
        let list_hashes = List.map Operation_list_hash.compute hashes in
        Lwt_list.iteri_p
          (fun i hashes ->
             let path = Operation_list_list_hash.compute_path list_hashes i in
             Store.Block.Operation_hashes.store
               (store, hash) i hashes >>= fun () ->
             Store.Block.Operation_path.store (store, hash) i path)
          hashes >>= fun () ->
        Lwt_list.iteri_p
          (fun i ops ->
             Store.Block.Operations.store (store, hash) i ops)
          operations >>= fun () ->
        Lwt_list.iteri_p
          (fun i ops ->
             Store.Block.Operations_metadata.store (store, hash) i ops)
          operations_metadata >>= fun () ->
        (* Store predecessors *)
        store_predecessors store hash >>= fun () ->
        (* Update the chain state. *)
        Shared.use chain_state.chain_data begin fun chain_data ->
          let store = chain_data.chain_data_store in
          let predecessor = block_header.shell.predecessor in
          Store.Chain_data.Known_heads.remove store predecessor >>= fun () ->
          Store.Chain_data.Known_heads.store store hash
        end >>= fun () ->
        let block = { chain_state ; hash ; contents ; header } in
        Lwt_watcher.notify chain_state.block_watcher block ;
        Lwt_watcher.notify chain_state.global_state.block_watcher block ;
        return_some block
      end
    end

  let store_invalid chain_state block_header errors =
    let bytes = Block_header.to_bytes block_header in
    let hash = Block_header.hash_raw bytes in
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Contents.known (store, hash) >>= fun known_valid ->
      fail_when known_valid (failure "Known valid") >>=? fun () ->
      Store.Block.Invalid_block.known store hash >>= fun known_invalid ->
      if known_invalid then
        return_false
      else
        Store.Block.Invalid_block.store store hash
          { level = block_header.shell.level ; errors } >>= fun () ->
        return_true
    end

  let watcher (state : chain_state) =
    Lwt_watcher.create_stream state.block_watcher

  let operation_hashes { chain_state ; hash ; header } i =
    if i < 0 || header.shell.validation_passes <= i then
      invalid_arg "State.Block.operations" ;
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Operation_hashes.read_exn (store, hash) i >>= fun hashes ->
      Store.Block.Operation_path.read_exn (store, hash) i >>= fun path ->
      Lwt.return (hashes, path)
    end

  let all_operation_hashes { chain_state ; hash ; header } =
    Shared.use chain_state.block_store begin fun store ->
      Lwt_list.map_p
        (Store.Block.Operation_hashes.read_exn (store, hash))
        (0 -- (header.shell.validation_passes - 1))
    end

  let operations { chain_state ; hash ; header } i =
    if i < 0 || header.shell.validation_passes <= i then
      invalid_arg "State.Block.operations" ;
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Operation_path.read_exn (store, hash) i >>= fun path ->
      Store.Block.Operations.read_exn (store, hash) i >>= fun ops ->
      Lwt.return (ops, path)
    end

  let operations_metadata { chain_state ; hash ; header } i =
    if i < 0 || header.shell.validation_passes <= i then
      invalid_arg "State.Block.operations_metadata" ;
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Operations_metadata.read_exn (store, hash) i >>= fun ops ->
      Lwt.return ops
    end

  let all_operations { chain_state ; hash ; header } =
    Shared.use chain_state.block_store begin fun store ->
      Lwt_list.map_p
        (fun i -> Store.Block.Operations.read_exn (store, hash) i)
        (0 -- (header.shell.validation_passes - 1))
    end

  let all_operations_metadata { chain_state ; hash ; header } =
    Shared.use chain_state.block_store begin fun store ->
      Lwt_list.map_p
        (fun i -> Store.Block.Operations_metadata.read_exn (store, hash) i)
        (0 -- (header.shell.validation_passes - 1))
    end

  let context { chain_state ; hash } =
    Shared.use chain_state.block_store begin fun block_store ->
      Store.Block.Contents.read_exn (block_store, hash)
    end >>= fun { context = commit } ->
    Shared.use chain_state.context_index begin fun context_index ->
      Context.checkout_exn context_index commit
    end

  let protocol_hash block =
    context block >>= fun context ->
    Context.get_protocol context

  let test_chain block =
    context block >>= fun context ->
    Context.get_test_chain context

  let block_validity chain_state block : Block_locator.validity Lwt.t =
    known chain_state block >>= function
    | false ->
        if Block_hash.equal block (Chain.faked_genesis_hash chain_state) then
          Lwt.return Block_locator.Known_valid
        else
          Lwt.return Block_locator.Unknown
    | true ->
        known_invalid chain_state block >>= function
        | true ->
            Lwt.return Block_locator.Known_invalid
        | false ->
            Lwt.return Block_locator.Known_valid

  let known chain_state hash =
    Shared.use chain_state.block_store begin fun store ->
      Store.Block.Contents.known (store, hash) >>= fun known ->
      if known then
        Lwt.return_true
      else
        Store.Block.Invalid_block.known store hash
    end

  let known_ancestor chain_state history_mode locator =
    Block_locator.unknown_prefix
      ~is_known:(block_validity chain_state) locator >>= function
    | `Ok (_tail, locator) -> Lwt.return_some locator
    | `Invalid -> Lwt.return_none
    | `Unknown -> begin match history_mode with
        | History_mode.Archive -> Lwt.return_none
        | _ ->
            (* We restrict this locator history later when calling to_steps. *)
            Lwt.return_some locator
      end

  let get_rpc_directory ({ chain_state ; _ } as block) =
    read_opt chain_state block.header.shell.predecessor >>= function
    | None -> Lwt.return_none (* genesis *)
    | Some pred ->
        protocol_hash pred >>= fun protocol ->
        match
          Protocol_hash.Table.find_opt
            chain_state.block_rpc_directories protocol
        with
        | None -> Lwt.return_none
        | Some map ->
            protocol_hash block >>= fun next_protocol ->
            Lwt.return (Protocol_hash.Map.find_opt next_protocol map)

  let set_rpc_directory ({ chain_state ; _ } as block) dir =
    read_exn chain_state block.header.shell.predecessor >>= fun pred ->
    protocol_hash block >>= fun next_protocol ->
    protocol_hash pred >>= fun protocol ->
    let map =
      Option.unopt ~default:Protocol_hash.Map.empty
        (Protocol_hash.Table.find_opt chain_state.block_rpc_directories protocol)
    in
    Protocol_hash.Table.replace
      chain_state.block_rpc_directories protocol
      (Protocol_hash.Map.add next_protocol dir map) ;
    Lwt.return_unit

end

let watcher (state : global_state) =
  Lwt_watcher.create_stream state.block_watcher

let read_block { global_data } hash =
  Shared.use global_data begin fun { chains } ->
    Chain_id.Table.fold
      (fun _chain_id chain_state acc ->
         acc >>= function
         | Some _ -> acc
         | None ->
             Block.read_opt chain_state hash >>= function
             | None -> acc
             | Some block -> Lwt.return_some block)
      chains
      Lwt.return_none
  end

let read_block_exn t hash =
  read_block t hash >>= function
  | None -> Lwt.fail Not_found
  | Some b -> Lwt.return b

let fork_testchain block protocol expiration =
  Shared.use block.chain_state.global_state.global_data begin fun data ->
    Block.context block >>= fun context ->
    Context.set_test_chain context Not_running >>= fun context ->
    Context.set_protocol context protocol >>= fun context ->
    Context.commit_test_chain_genesis
      data.context_index block.hash block.header.shell.timestamp
      context >>=? fun (chain_id, genesis, commit) ->
    let genesis = {
      block = genesis ;
      time = Time.add block.header.shell.timestamp 1L ;
      protocol ;
    } in
    Chain.locked_create block.chain_state.global_state data
      chain_id ~expiration genesis commit >>= fun chain ->
    update_chain_data block.chain_state begin fun _ chain_data ->
      Lwt.return (Some { chain_data with test_chain = Some chain.chain_id }, ())
    end >>= fun () ->
    return chain
  end

let best_known_head_for_checkpoint chain_state checkpoint =
  Shared.use chain_state.block_store begin fun store ->
    Shared.use chain_state.chain_data begin fun data ->
      let head_hash = data.data.current_head.hash in
      let head_header = data.data.current_head.header in
      Locked_block.is_valid_for_checkpoint
        store head_hash head_header checkpoint >>= fun valid ->
      if valid then
        Lwt.return data.data.current_head
      else
        let find_valid_predecessor hash =
          Store.Block.Header.read_exn
            (store, hash) >>= fun header ->
          if Compare.Int32.(header.shell.level < checkpoint.shell.level) then
            Store.Block.Contents.read_exn
              (store, hash) >>= fun contents ->
            Lwt.return { hash ; contents ; chain_state ; header }
          else
            predecessor_n store hash
              (1 + (Int32.to_int @@
                    Int32.sub header.shell.level checkpoint.shell.level)) >>= function
            | None -> assert false
            | Some pred ->
                Store.Block.Contents.read_exn
                  (store, pred) >>= fun pred_contents ->
                Store.Block.Header.read_exn
                  (store, pred) >>= fun pred_header ->
                Lwt.return { hash = pred ; contents = pred_contents ;
                             chain_state ; header = pred_header } in
        Store.Chain_data.Known_heads.read_all
          data.chain_data_store >>= fun heads ->
        Store.Block.Contents.read_exn
          (store, chain_state.genesis.block) >>= fun genesis_contents ->
        Store.Block.Header.read_exn
          (store, chain_state.genesis.block) >>= fun genesis_header ->
        let genesis =
          { hash = chain_state.genesis.block ;
            contents = genesis_contents ;
            chain_state ; header = genesis_header } in
        Block_hash.Set.fold
          (fun head best ->
             let valid_predecessor = find_valid_predecessor head in
             best >>= fun best ->
             valid_predecessor >>= fun pred ->
             if Fitness.(pred.header.shell.fitness >
                         best.header.shell.fitness) then
               Lwt.return pred
             else
               Lwt.return best)
          heads
          (Lwt.return genesis)
    end
  end

module Protocol = struct

  include Protocol

  let known global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.known store hash
    end

  let read global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.read store hash
    end
  let read_opt global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.read_opt store hash
    end
  let read_exn global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.read_exn store hash
    end

  let read_raw global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.RawContents.read (store, hash)
    end
  let read_raw_opt global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.RawContents.read_opt (store, hash)
    end
  let read_raw_exn global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.RawContents.read_exn (store, hash)
    end

  let store global_state p =
    let bytes = Protocol.to_bytes p in
    let hash = Protocol.hash_raw bytes in
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.known store hash >>= fun known ->
      if known then
        Lwt.return_none
      else
        Store.Protocol.RawContents.store (store, hash) bytes >>= fun () ->
        Lwt_watcher.notify global_state.protocol_watcher hash ;
        Lwt.return_some hash
    end

  let remove global_state hash =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.known store hash >>= fun known ->
      if known then
        Lwt.return_false
      else
        Store.Protocol.Contents.remove store hash >>= fun () ->
        Lwt.return_true
    end

  let list global_state =
    Shared.use global_state.protocol_store begin fun store ->
      Store.Protocol.Contents.fold_keys store
        ~init:Protocol_hash.Set.empty
        ~f:(fun x acc -> Lwt.return (Protocol_hash.Set.add x acc))
    end

  let watcher (state : global_state) =
    Lwt_watcher.create_stream state.protocol_watcher

end

module Current_mempool = struct

  let set chain_state ~head mempool =
    update_chain_data chain_state begin fun _chain_data_store data ->
      if Block_hash.equal head (Block.hash data.current_head) then
        Lwt.return (Some { data with current_mempool = mempool }, ())
      else
        Lwt.return (None, ())
    end

  let get chain_state =
    read_chain_data chain_state begin fun _chain_data_store data ->
      Lwt.return (Block.header data.current_head, data.current_mempool)
    end

end

let may_create_chain state chain genesis =
  Chain.get state chain >>= function
  | Ok chain -> Lwt.return chain
  | Error _ ->
      Chain.create
        ~allow_forked_chain:true
        state genesis

let read
    global_store
    context_index
    main_chain =
  let global_data = {
    chains = Chain_id.Table.create 17 ;
    global_store ;
    context_index ;
  } in
  let state = {
    global_data = Shared.create global_data ;
    protocol_store = Shared.create @@ Store.Protocol.get global_store ;
    main_chain ;
    protocol_watcher = Lwt_watcher.create_input () ;
    block_watcher = Lwt_watcher.create_input () ;
  } in
  Chain.read_all state >>=? fun () ->
  return state

type error += Wrong_coercion_history_mode of { previous_mode: History_mode.t; next_mode: History_mode.t }

let () = register_error_kind `Permanent
    ~id:"node_config_file.wrong_coercion_history_mode"
    ~title:"Wrong coercion between history modes"
    ~description:"Wrong coercion between history modes."
    ~pp:(fun ppf (hm1, hm2) ->
        Format.fprintf ppf
          "@[Cannot update node history mode from %a mode to %a mode.@]"
          History_mode.pp hm1 History_mode.pp hm2
      )
    (Data_encoding.obj2
       (Data_encoding.req "previous_mode" History_mode.encoding)
       (Data_encoding.req "next_mode" History_mode.encoding))
    (function Wrong_coercion_history_mode x -> Some (x.previous_mode, x.next_mode)
            | _ -> None)
    (fun (previous_mode, next_mode) -> Wrong_coercion_history_mode { previous_mode; next_mode })

let history_mode_tag =
  Tag.def "history_mode" History_mode.pp

let check_and_save_history_mode
    ~previous_mode
    ~next_mode
    global_store
    state
  =
  let open History_mode in
  match (previous_mode, next_mode) with
  | (Archive, Archive) | (Full, Full) | (Rolling, Rolling) ->
      return_unit
  | (Full, Archive) | (Rolling, Archive) | (Rolling, Full) ->
      fail (Wrong_coercion_history_mode { previous_mode ; next_mode })
  | (Archive, Full) ->
      lwt_log_notice Tag.DSL.(fun f ->
          f "Cleaning up the state to switch to %a mode..."
          -% t event "cleanup_state"
          -% a history_mode_tag Full) >>= fun () ->
      Store.Configuration.History_mode.store
        global_store Full >>= fun () ->
      Chain.all state >>= fun chains ->
      iter_s (fun chain_state ->
          Chain.checkpoint chain_state >>= fun checkpoint ->
          let hash = Block_header.hash checkpoint in
          let faked_genesis_hash = Chain.faked_genesis_hash chain_state in
          if Block_hash.equal hash faked_genesis_hash then
            return_unit
          else
            let lvl = checkpoint.shell.level in
            Chain.purge_full chain_state (lvl, hash) >>= fun () ->
            return_unit
        ) chains >>=? fun () ->
      return_unit
  | (Archive, Rolling) | (Full, Rolling) ->
      lwt_log_notice Tag.DSL.(fun f ->
          f "Cleaning up the state to switch to %a mode..."
          -% t event "cleanup_state"
          -% a history_mode_tag Rolling) >>= fun () ->
      Store.Configuration.History_mode.store
        global_store Rolling >>= fun () ->
      Chain.all state >>= fun chains ->
      iter_s (fun chain_state ->
          Chain.checkpoint chain_state >>= fun checkpoint ->
          let hash = Block_header.hash checkpoint in
          let faked_genesis_hash = Chain.faked_genesis_hash chain_state in
          if Block_hash.equal hash faked_genesis_hash then
            return_unit
          else
            let lvl = checkpoint.shell.level in
            Chain.purge_rolling chain_state (lvl, hash)
            >>= fun () ->
            return_unit
        ) chains >>=? fun () ->
      return_unit

let init
    ?patch_context
    ?(store_mapsize=40_960_000_000L)
    ?(context_mapsize=409_600_000_000L)
    ~store_root
    ~context_root
    ?history_mode
    genesis =
  Store.init ~mapsize:store_mapsize store_root >>=? fun global_store ->
  Context.init
    ~mapsize:context_mapsize ?patch_context
    context_root >>= fun context_index ->
  let main_chain = Chain_id.of_block_hash genesis.Chain.block in
  read global_store context_index main_chain >>=? fun state ->
  may_create_chain state main_chain genesis >>= fun main_chain_state ->
  Store.Configuration.History_mode.read_opt global_store >>= begin function
    | None ->
        let mode = Option.unopt ~default:History_mode.Full history_mode in
        Store.Configuration.History_mode.store global_store mode >>= fun () ->
        return mode
    | Some previous_history_mode ->
        match history_mode with
        | None -> return previous_history_mode
        | Some history_mode ->
            check_and_save_history_mode
              ~previous_mode:previous_history_mode
              ~next_mode:history_mode global_store state >>=? fun () ->
            return history_mode
  end >>=? fun history_mode ->
  return (state, main_chain_state, context_index, history_mode)

let history_mode { global_data } =
  Shared.use global_data begin fun { global_store } ->
    Store.Configuration.History_mode.read global_store
  end

let close { global_data } =
  Context.close () >>= fun () ->
  Shared.use global_data begin fun { global_store } ->
    Store.close global_store ;
    Lwt.return_unit
  end

let upgrade_0_0_1
    ?(store_mapsize=4_096_000_000_000L)
    ~store_root () =
  Store.init ~mapsize:store_mapsize store_root >>=? fun global_store ->
  Store.Chain.list global_store >>= fun chains ->
  iter_s
    begin fun chain_id ->
      Format.eprintf "Upgrading block storage for chain %a...@." Chain_id.pp chain_id ;
      let chain_store = Store.Chain.get global_store chain_id in
      let block_store = Store.Block.get chain_store in
      let chain_data_store = Store.Chain_data.get chain_store in
      Format.eprintf "Listing blocks..." ;
      if Unix.isatty Unix.stderr then
        Format.eprintf "%!\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b%!"
      else Format.eprintf "\n%!" ;
      Store.Block.fold ~init:(0, []) ~f:begin fun h (listed, acc) ->
        if listed mod 1000 = 0 then begin
          Format.eprintf "%dK blocks listed..." (listed / 1000);
          if Unix.isatty Unix.stderr then
            Format.eprintf "%!\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b%!"
          else Format.eprintf "\n%!"
        end ;
        Lwt.return (listed + 1, h :: acc)
      end block_store >>= fun (_, blocks) ->
      Format.eprintf "Processing blocks..." ;
      if Unix.isatty Unix.stderr then
        Format.eprintf "%!\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b%!"
      else Format.eprintf "\n%!" ;
      let rec loop_on_chunks blocks (erroneous, skipped, processed) =
        let blocks, rest = List.split_n 1000 blocks in
        Store.with_atomic_rw global_store begin fun () ->
          fold_left_s begin fun (erroneous, skipped, processed) h ->
            begin
              Store.Block.Contents_0_0_1.read
                (block_store, h) >>=? fun (header, contents) ->
              Store.Block.Header.store (block_store, h) header >>= fun () ->
              Store.Block.Contents.store (block_store, h) contents >>= fun () ->
              if (processed + skipped) mod 1000 = 0 then begin
                Format.eprintf "%dK blocks processed..." ((processed + skipped) / 1000);
                if Unix.isatty Unix.stderr then
                  Format.eprintf "%!\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b%!"
                else Format.eprintf "\n%!"
              end ;
              return ()
            end >>= function
            | Ok () -> return (erroneous, skipped, processed + 1)
            | Error _ ->
                Store.Block.Header.read_opt (block_store, h) >>= fun header ->
                Store.Block.Contents.read_opt (block_store, h) >>= fun contents ->
                match header, contents with
                | Some _, Some _ -> (* already converted *) return (erroneous, skipped + 1, processed)
                | None, None ->
                    Store.Block.Operations_metadata.keys (block_store, h) >>= fun stale_metadata ->
                    if stale_metadata <> [] then
                      Store.Block.Predecessors.remove_all (block_store, h) >>= fun () ->
                      Store.Block.Operations_metadata.remove_all (block_store, h) >>= fun () ->
                      return (erroneous, skipped + 1, processed)
                    else
                      return (h :: erroneous, skipped, processed)
                | _ -> return (h :: erroneous, skipped, processed)
          end (erroneous, skipped, processed) blocks
        end >>=? fun (erroneous, skipped, processed) ->
        if rest = [] then return (erroneous, skipped, processed)
        else loop_on_chunks rest (erroneous, skipped, processed) in
      loop_on_chunks blocks ([], 0, 0) >>=? fun (erroneous, skipped, processed) ->
      Format.eprintf
        "Done Processing %d blocks (%d already converted were skipped).\n%!"
        processed skipped ;
      begin match erroneous with
        | [] -> ()
        | blocks ->
            Format.eprintf
              "Some blocks were corrupted. Consider resynchronizing.@.\
               @[<v 0>%a@]@."
              (Format.pp_print_list Block_hash.pp) blocks
      end ;
      Format.eprintf "Upgrading checkpoint for chain %a...@." Chain_id.pp chain_id ;
      Store.Chain_data.Checkpoint_0_0_1.read_opt chain_data_store >>= begin function
        | None ->
            Store.Chain_data.Checkpoint_0_0_1.remove chain_data_store >>= fun () ->
            return_unit
        | Some (_level, hash) ->
            Store.Block.Header.read (block_store, hash) >>=? fun header ->
            Store.Chain_data.Checkpoint.store chain_data_store header >>= fun () ->
            return_unit
      end >>=? fun () ->
      Store.Chain.Genesis_hash.read chain_store >>=? fun genesis_hash ->
      Store.Chain_data.Save_point.store chain_data_store (0l, genesis_hash) >>= fun () ->
      Store.Chain_data.Caboose.store chain_data_store (0l, genesis_hash) >>= fun () ->
      return_unit
    end
    chains >>=? fun () ->
  Format.eprintf "Initializing history mode to: %a node@." History_mode.pp History_mode.Archive ;
  Store.Configuration.History_mode.store global_store History_mode.Archive >>= fun () ->
  Store.close global_store ;
  return_unit
