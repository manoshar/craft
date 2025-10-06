defmodule Craft.Consensus.State.LeaderState do
  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.Log.MembershipEntry
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Persistence
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.InstallSnapshot
  alias Craft.SnapshotServer.SnapshotTransfer

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]

  defstruct [
    :next_indices,
    :match_indices,
    :membership_change,
    :leadership_transfer,
    :last_heartbeat_sent_at, # the time that the most recent heartbeat round was sent
    :last_quorum_at, # the last time we knew we were leader
    # indicates if the current quorum round has been successful thus far (so we don't tell the machine multiple times)
    :current_quorum_successful,
    :waiting_for_lease,
    last_heartbeat_replies_at: %{}, # for CheckQuorum, voting members only
    snapshot_transfers: %{}
  ]

  defmodule MembershipChange do
    # action: :add | :remove
    defstruct [:action, :node, :log_index]
  end

  defmodule LeadershipTransfer do
    defstruct [
      :from, # {pid, ref}, from Consensus.cast, for transmission to the new leader via AppendEntries
      :current_candidate,
      candidates: MapSet.new()
    ]

    def new(transfer_to, from) do
      %__MODULE__{current_candidate: transfer_to, from: from}
    end

    def new(%State{} = state) do
      %__MODULE__{
        candidates: state.members.voting_nodes,
        from: :internal
      }
      |> next_transfer_candidate()
    end

    def next_transfer_candidate(%__MODULE__{} = leadership_transfer) do
      if Enum.empty?(leadership_transfer.candidates) do
        :error
      else
        candidate = Enum.random(leadership_transfer.candidates)
        candidates = MapSet.delete(leadership_transfer.candidates, candidate)

        %{leadership_transfer | current_candidate: candidate, candidates: candidates}
      end
    end
  end

  # FIXME: if config_change_in_progress, reconstruct :membership_change?
  # this may need to happen after new leader figures out the commit index
  # may need to have stored the :membership_change in the MembershipEntry
  def new(%State{} = state) do
    next_index = Persistence.latest_index(state.persistence) + 1
    next_indices = state.members |> Members.other_nodes() |> Map.new(&{&1, next_index})
    match_indices = state.members |> Members.other_nodes() |> Map.new(&{&1, 0})

    %__MODULE__{
      next_indices: next_indices,
      match_indices: match_indices,
      last_quorum_at: :erlang.monotonic_time(:millisecond),
      last_heartbeat_sent_at: :erlang.monotonic_time(:millisecond)
    }
  end

  def config_change_in_progress?(%State{} = state) do
    state.persistence
    |> Persistence.fetch_from(state.commit_index + 1)
    |> Enum.any?(fn
      %MembershipEntry{} -> true
      _ -> false
    end)
  end

  def add_node(%State{} = state, node, log_index) do
    next_index = Persistence.latest_index(state.persistence) + 1
    next_indices = Map.put(state.leader_state.next_indices, node, next_index)
    match_indices = Map.put(state.leader_state.match_indices, node, 0)

    membership_change = %MembershipChange{action: :add, node: node, log_index: log_index}

    leader_state =
      %{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change
      }

    %{state | members: Members.add_member(state.members, node), leader_state: leader_state}
  end

  def remove_node(%State{} = state, node, log_index) do
    next_indices = Map.delete(state.leader_state.next_indices, node)
    match_indices = Map.delete(state.leader_state.match_indices, node)
    last_heartbeat_replies_at = Map.delete(state.leader_state.last_heartbeat_replies_at, node)

    membership_change = %MembershipChange{action: :remove, node: node, log_index: log_index}

    snapshot_transfers = Map.delete(state.leader_state.snapshot_transfers, node)

    leader_state =
      %{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change,
        last_heartbeat_replies_at: last_heartbeat_replies_at,
        snapshot_transfers: snapshot_transfers
      }

    %{state | members: Members.remove_member(state.members, node), leader_state: leader_state}
  end

  # this approach of generating a new id for each consensus round and only accepting replies from that one round
  # may cause quorum failures on unreliable networks, if this is an issue, we can implement a sliding window of quorum rounds
  # then `last_quorum_at` is just the most recent quorum round to complete.
  def handle_append_entries_results(%State{leader_state: %__MODULE__{last_heartbeat_sent_at: round_time}} = state, %AppendEntries.Results{heartbeat_sent_at: reply_time} = results) when round_time != reply_time do
    Logger.debug("heartbeat from #{results.from} missed deadline, ignoring.", logger_metadata(state))

    state
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{} = results) do
    heartbeat_sent_at = results.heartbeat_sent_at

    case Map.fetch(state.leader_state.last_heartbeat_replies_at, results.from) do
      {:ok, {^heartbeat_sent_at, _}} ->
        Logger.warning("duplicate heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))

        state

      _ ->
        state = do_handle_append_entries_results(state, results)

        MemberCache.update(state)

        state
    end
  end

  defp do_handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: true} = results) do
    state = bump_last_heartbeat_reply_at(state, results)
    # accounts for the possibility of stale AppendEntries results (due to pathological network reordering)
    # and also avoids work when no follower log appends took place (i.e. a heartbeat that doesnt append anything)
    if results.latest_index > state.leader_state.match_indices[results.from] do
      match_indices = Map.put(state.leader_state.match_indices, results.from, results.latest_index)
      next_indices = Map.put(state.leader_state.next_indices, results.from, results.latest_index + 1)
      state = %{state | leader_state: %{state.leader_state | next_indices: next_indices, match_indices: match_indices}}
      # find the highest uncommitted match index shared by a majority of servers
      #
      # when we become leader, match indexes work their way up from zero non-uniformly
      # so it's entirely possible that we don't find a quorum of followers with a match index
      # until match indexes work their way up to parity
      #
      # this node (the leader), might not be voting in majorities if it is removing itself
      # from the cluster (section 4.2.2)
      #
      match_indices_for_commitment =
        if Members.this_node_can_vote?(state.members) do
          Map.put(state.leader_state.match_indices, node(), Persistence.latest_index(state.persistence))
        else
          state.leader_state.match_indices
        end

      highest_uncommitted_match_index =
        match_indices_for_commitment
        |> Map.values()
        |> Enum.filter(fn index -> index >= state.commit_index end)
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.reverse()
        |> Enum.find(fn index ->
          num_members_with_index = Enum.count(match_indices_for_commitment, fn {_node, match_index} -> match_index >= index end)

          num_members_with_index >= State.quorum_needed(state)
        end)

      # only bump commit index when the quorum entry is from the current term (section 5.4.2)
      with false <- is_nil(highest_uncommitted_match_index),
           {:ok, entry} <- Persistence.fetch(state.persistence, highest_uncommitted_match_index),
           true <- entry.term == state.current_term do
        %{state | commit_index: highest_uncommitted_match_index}
      else
        _ ->
          state
      end
    else
      state
    end
  end

  defp do_handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: false} = results) do
    state = bump_last_heartbeat_reply_at(state, results)
    # we don't know where we match the followers log
    match_indices = Map.put(state.leader_state.match_indices, results.from, 0)
    state = put_in(state.leader_state.match_indices, match_indices)

    # is the follower going to need a snapshot?
    if needs_snapshot?(state, results.from) do
      if state.machine.__craft_mutable__() do
        {:needs_snapshot, create_snapshot_transfer(state, results.from)}
      else
        {:needs_snapshot, state}
      end
    else
      next_indices = Map.update!(state.leader_state.next_indices, results.from, fn next_index -> next_index - 1 end)

      put_in(state.leader_state.next_indices, next_indices)
    end
  end

  def create_snapshot_transfer(%State{leader_state: %__MODULE__{snapshot_transfers: snapshot_transfers}} = state, for_node) when is_map_key(snapshot_transfers, for_node), do: state

  def create_snapshot_transfer(%State{} = state, for_node) do
    {index, {remote_path, files}} = state.snapshot
    snapshot_transfer = SnapshotTransfer.new(remote_path, files)

    snapshot_transfers = Map.put(state.leader_state.snapshot_transfers, for_node, {index, snapshot_transfer})

    put_in(state.leader_state.snapshot_transfers, snapshot_transfers)
  end

  def handle_install_snapshot_results(%State{} = state, %InstallSnapshot.Results{success: true} = results) do
    snapshot_transfers = Map.delete(state.leader_state.snapshot_transfers, results.from)

    leader_state =
      %{
        state.leader_state |
        snapshot_transfers: snapshot_transfers,
        match_indices: Map.put(state.leader_state.match_indices, results.from, results.latest_index),
        next_indices: Map.put(state.leader_state.next_indices, results.from, results.latest_index + 1)
      }

    %{state | leader_state: leader_state}
  end

  def needs_snapshot?(%State{} = state, node) do
    not match?({:ok, _}, Persistence.fetch(state.persistence, state.leader_state.next_indices[node] - 1))
  end

  def sending_snapshot?(%State{} = state, node) do
    !!state.leader_state.snapshot_transfers[node]
  end

  def transfer_leadership(%State{} = state) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(state))
  end

  def transfer_leadership(%State{} = state, to_member, from \\ nil) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(to_member, from))
  end

  def bump_last_heartbeat_reply_at(%State{} = state, %AppendEntries.Results{} = results) do
    if Members.can_vote?(state.members, results.from) do
      reply_received_at = :erlang.monotonic_time(:millisecond)
      last_heartbeat_replies_at = Map.put(state.leader_state.last_heartbeat_replies_at, results.from, {results.heartbeat_sent_at, reply_received_at})
      state = put_in(state.leader_state.last_heartbeat_replies_at, last_heartbeat_replies_at)

      htt_ms = reply_received_at - results.heartbeat_sent_at
      Logger.info("heartbeat-stats - from: #{results.from}, heartbeat_sent_at: #{results.heartbeat_sent_at}, heartbeat_received_at: #{reply_received_at}, HTT=#{htt_ms}ms")

      # -1 since we're the leader
      num_replies_needed = State.quorum_needed(state) - 1

      num_heartbeat_replies_received_this_round =
        Enum.count(last_heartbeat_replies_at, fn {_member, {sent_at, _received_at}} ->
          sent_at == state.leader_state.last_heartbeat_sent_at
        end)

      # if quorum was achieved, the most we can say is that we we were leader when the round was sent
      if num_heartbeat_replies_received_this_round >= num_replies_needed do
        state = put_in(state.leader_state.last_quorum_at, state.leader_state.last_heartbeat_sent_at)

        if not state.leader_state.current_quorum_successful do
          # snapshotting truncates the log, so we want to make sure that all followers are caught up first
          # we don't want to delete a snapshot that's being downloaded, nor truncate the log before a follower
          # that's just pulled a snapshot can catch up
          # TODO: make log length configurable
          all_followers_caught_up = Enum.empty?(state.members.catching_up_nodes)
          log_too_long = Persistence.length(state.persistence) > 20
          # log_too_big = Persistence.log_size() > 100mb or 100 entries, etc

          Machine.quorum_reached(state, all_followers_caught_up && log_too_long)

          put_in(state.leader_state.current_quorum_successful, true)
        else
          state
        end
      else
        state
      end
    else
      state
    end
  end
end
