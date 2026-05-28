# frozen_string_literal: true

module Reports
  # Peer-review report composed of three streaming reducers.
  #
  # Three reducers build the report:
  #
  #   ReviewersReducer — streams ReviewResponseMap rows to populate reviewer
  #     identity fields (id, user_id, name, handle) in shared state.
  #
  #   ScoresReducer — streams submitted Responses to compute score percentages
  #     per reviewee per round, written into shared state under each reviewer.
  #
  #   Both write into a shared state keyed by reviewer_id so reviewer info and
  #   scores are co-located without a merge loop:
  #     { reviewer_id => { id:, user_id:, name:, handle:,
  #                        scores: { reviewee_id => { round => score_pct } } } }
  #
  #   AvgRangesReducer — runs independently, streams AssignmentTeam rows to
  #     compute the average review score per team:
  #     { team_id => avg_score }
  #
  # Each reducer streams its source via find_each — no full result set is ever
  # materialized in Ruby at once.
  class ReviewReport
    def self.for_assignment(assignment)
      new(assignment)
    end

    def initialize(reportable)
      @reportable = reportable
    end

    # Runs all three reducers and returns the combined report:
    #   {
    #     reviewers: [{ id:, user_id:, name:, handle:,
    #                   scores: { reviewee_id => { round => score_pct } } }, ...],
    #     avg_and_ranges: { team_id => avg_score }
    #   }
    def run
      shared_state = Hash.new do |state, reviewer_id|
        state[reviewer_id] = {
          id:      nil,
          user_id: nil,
          name:    nil,
          handle:  nil,
          scores:  Hash.new { |by_reviewee, reviewee_id| by_reviewee[reviewee_id] = {} }
        }
      end

      ReviewersReducer.new(@reportable).run(shared_state)
      ScoresReducer.new(@reportable).run(shared_state)

      {
        reviewers:      shared_state.values.sort_by { |r| r[:name].to_s.downcase },
        avg_and_ranges: AvgRangesReducer.new(@reportable).run
      }
    end

    # -----------------------------------------------------------------------
    # Reducer 1 — reviewer info.
    # Streams ReviewResponseMap rows; writes reviewer details into shared state
    # on first occurrence per reviewer.
    # -----------------------------------------------------------------------
    class ReviewersReducer < BaseReport
      def source
        ReviewResponseMap.for_assignment(@reportable.id).includes(reviewer: :user)
      end

      def initial_state = {}

      def accumulate(state, response_map)
        reviewer_id = response_map.reviewer_id
        return if state.key?(reviewer_id)

        reviewer = response_map.reviewer
        return unless reviewer

        state[reviewer_id][:id]      = reviewer.id
        state[reviewer_id][:user_id] = reviewer.user_id
        state[reviewer_id][:name]    = reviewer.user&.name
        state[reviewer_id][:handle]  = reviewer.handle
      end
    end

    # -----------------------------------------------------------------------
    # Reducer 2 — per-reviewer × reviewee × round score percentages.
    # Streams submitted Responses with scores and questionnaires eagerly loaded
    # to avoid N+1 inside calculate_total_score and maximum_score.
    # Writes score_pct into shared state under
    # state[reviewer_id][:scores][reviewee_id][round].
    # -----------------------------------------------------------------------
    class ScoresReducer < BaseReport
      def source
        Response
          .submitted_review_responses_for(@reportable.id)
          .includes(:response_map, scores: { item: :questionnaire })
      end

      def initial_state = {}

      def accumulate(state, response)
        return if response.maximum_score.zero?

        reviewer_id = response.response_map.reviewer_id
        reviewee_id = response.response_map.reviewee_id
        round       = response.round || 1
        score_pct   = (response.calculate_total_score.to_f / response.maximum_score * 100).round(2)

        state[reviewer_id][:scores][reviewee_id][round] = score_pct
      end
    end

    # -----------------------------------------------------------------------
    # Reducer 3 — per-team average review score.
    # Streams AssignmentTeam rows with review_mappings, responses, and scores
    # eagerly loaded to avoid N+1. Delegates score computation to
    # aggregate_review_grade (via ReviewAggregator concern) which picks the
    # latest submitted response per round per map and normalizes the score.
    # Output: { team_id => avg_score }
    # -----------------------------------------------------------------------
    class AvgRangesReducer < BaseReport
      def source
        AssignmentTeam
          .where(parent_id: @reportable.id)
          .includes(review_mappings: { responses: { scores: :item } })
      end

      def initial_state = {}

      def accumulate(state, team)
        state[team.id] = team.aggregate_review_grade
      end
    end
  end
end
