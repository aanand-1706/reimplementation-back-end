# frozen_string_literal: true

module Reports
  # Peer-review report composed of three independent streaming pipelines:
  # 1 ReviewersPipeline — distinct reviewers with user details
  # 2 ScoresPipeline — per-reviewer × round × reviewee score pct
  # 3 AvgRangesPipeline — per-team × round max / min / avg
  #
  # Each pipeline streams its own source relation via find_each, so no
  # full result set is ever materialised in Ruby at once.
  # max_question_score is precomputed once in #run and passed to both score
  # pipelines to avoid a duplicate DB query and N+1 inside the accumulate loop.
  class ReviewReport
    def self.for_assignment(assignment)
      new(assignment)
    end

    def initialize(reportable)
      @reportable = reportable
    end

    def run
      # Precomputed once and passed to both score pipelines to avoid a duplicate DB query.
      max_q_scores = AssignmentQuestionnaire
        .for_assignment_and_type(@reportable.id, 'ReviewQuestionnaire')
        .pluck(:used_in_round, 'questionnaires.max_question_score')
        .to_h
      {
        reviewers:      ReviewersPipeline.new(@reportable).run,
        review_scores:  ScoresPipeline.new(@reportable, max_q_scores).run,
        avg_and_ranges: AvgRangesPipeline.new(@reportable, max_q_scores).run
      }
    end

    # Shared source for ScoresPipeline and AvgRangesPipeline.
    # Both pipelines stream the same submitted ReviewResponseMap responses.
    module ReviewResponsePipelineShared
      def source
        Response
          .submitted_review_responses_for(@reportable.id)
          .includes(:response_map, scores: :item)
      end
    end

    # -----------------------------------------------------------------------
    # Pipeline 1 — distinct reviewers sorted by full name.
    # Groups by reviewer_id; first occurrence per reviewer is kept.
    # -----------------------------------------------------------------------
    class ReviewersPipeline < BaseReport
      def source
        ReviewResponseMap.for_assignment(@reportable.id).includes(reviewer: :user)
      end

      def grouper = ->(response_map) { response_map.reviewer_id }

      def initial_state = {}

      def accumulate(state, reviewer_id, response_map)
        return if state.key?(reviewer_id)

        reviewer = response_map.reviewer
        return unless reviewer

        state[reviewer_id] = {
          id: reviewer.id,
          user_id: reviewer.user_id,
          name: reviewer.user&.name,
          full_name: reviewer.user&.full_name,
          handle: reviewer.handle
        }
      end

      def finalize(state)
        state.values.sort_by { |reviewer| reviewer[:full_name].to_s.downcase }
      end
    end

    # -----------------------------------------------------------------------
    # Pipeline 2 — per-reviewer × round × reviewee score percentages.
    # Streams submitted Responses with scores and items eagerly loaded.
    # Output: { reviewer_id => { round => { reviewee_id => pct } } }
    # -----------------------------------------------------------------------
    class ScoresPipeline < BaseReport
      include ReviewResponsePipelineShared

      def initialize(reportable, max_q_scores)
        super(reportable)
        @max_q_score = max_q_scores
      end

      def grouper = ->(response) { response.response_map.reviewer_id }

      def initial_state
        # Three-level nested hash: reviewer_id => round => reviewee_id => score_pct
        # Each level auto-initializes when a missing key is accessed,
        # so state[reviewer_id][round][reviewee_id] = pct never raises NoMethodError.
        Hash.new do |by_reviewer, reviewer_id|
          by_reviewer[reviewer_id] = Hash.new do |by_round, round|
            by_round[round] = {}
          end
        end
      end

      def accumulate(state, reviewer_id, response)
        answered = response.scores.reject { |s| s.answer.nil? }
        total_wt = answered.sum { |s| s.item.weight }
        return if total_wt.zero?

        round = response.round || 1
        reviewee_id = response.response_map.reviewee_id
        raw_score = answered.sum { |s| s.answer * s.item.weight }
        max_score = total_wt * (@max_q_score[round] || @max_q_score[nil] || 1)
        return unless max_score.positive?

        state[reviewer_id][round][reviewee_id] = ((raw_score.to_f / max_score) * 100).round(2)
      end
    end

    # -----------------------------------------------------------------------
    # Pipeline 3 — per-team × round aggregate (max / min / avg).
    # Same response stream as Pipeline 2; groups by reviewee_id.
    # Output: { team_id => { round => { max:, min:, avg: } } }
    # -----------------------------------------------------------------------
    # Check with prof how they are planning to use this information?? Is it for AVG score column?
    class AvgRangesPipeline < BaseReport
      include ReviewResponsePipelineShared

      def initialize(reportable, max_q_scores)
        super(reportable)
        @max_q_score = max_q_scores
      end

      def grouper = ->(response) { response.response_map.reviewee_id }

      def initial_state
        # Two-level nested hash: reviewee_id => round => [score_pcts]
        # Each level auto-initializes when a missing key is accessed.
        Hash.new do |by_team, reviewee_id|
          by_team[reviewee_id] = Hash.new { |by_round, round| by_round[round] = [] }
        end
      end

      def accumulate(state, reviewee_id, response)
        answered = response.scores.reject { |s| s.answer.nil? }
        total_wt = answered.sum { |s| s.item.weight }
        return if total_wt.zero?

        round = response.round || 1
        raw_score = answered.sum { |s| s.answer * s.item.weight }
        max_score = total_wt * (@max_q_score[round] || @max_q_score[nil] || 1)
        return unless max_score.positive?

        state[reviewee_id][round] << ((raw_score.to_f / max_score) * 100)
      end

      def finalize(state)
        state.transform_values do |rounds|
          rounds.transform_values do |scores|
            {
              max: scores.max.round(2),
              min: scores.min.round(2),
              avg: (scores.sum / scores.size).round(2)
            }
          end
        end
      end
    end
  end
end
