# frozen_string_literal: true

module Reports
  # Peer-review report composed of three independent streaming pipelines:
  #   1. ReviewersPipeline   — distinct reviewers with user details
  #   2. ScoresPipeline      — per-reviewer × round × reviewee score pct
  #   3. AvgRangesPipeline   — per-team × round max / min / avg
  #
  # Each pipeline streams its own source relation via find_each, so no
  # full result set is ever materialised in Ruby at once.
  # max_question_score is precomputed once per (assignment, round) to avoid
  # an N+1 query inside the hot accumulate loop.
  class ReviewReport
    # Factory method for assignment-scoped reports.
    def self.for_assignment(assignment)
      new(assignment)
    end

    # Factory method for course-scoped reports.
    def self.for_course(course)
      new(course)
    end

    # @param reportable [Assignment, Course] the object the report is scoped to.
    def initialize(reportable)
      @reportable = reportable
    end

    def run
      {
        reviewers:      ReviewersPipeline.new(@reportable).run,
        review_scores:  ScoresPipeline.new(@reportable).run,
        avg_and_ranges: AvgRangesPipeline.new(@reportable).run
      }
    end

    # -----------------------------------------------------------------------
    # Pipeline 1 — distinct reviewers sorted by full name.
    # Groups by reviewer_id; first occurrence per reviewer is kept.
    # -----------------------------------------------------------------------
    class ReviewersPipeline < BaseReport
      def source
        ReviewResponseMap
          .where(reviewed_object_id: @reportable.id)
          .includes(reviewer: :user)
      end

      # It pre-computes the grouping key once and passes it to accumulate so the subclass doesn't have to recompute it.
      def grouper       = ->(map) { map.reviewer_id }
      def initial_state = {}

      def accumulate(state, reviewer_id, map)
        return if state.key?(reviewer_id)

        r = map.reviewer
        return unless r

        state[reviewer_id] = {
          id:        r.id,
          user_id:   r.user_id,
          name:      r.user&.name,
          full_name: r.user&.full_name,
          handle:    r.handle
        }
      end

      def finalize(state)
        state.values.sort_by { |r| r[:full_name].to_s.downcase }
      end
    end

    # -----------------------------------------------------------------------
    # Pipeline 2 — per-reviewer × round × reviewee score percentages.
    # Streams submitted Responses with scores and items eagerly loaded.
    # Output: { reviewer_id => { round => { reviewee_id => pct } } }
    # -----------------------------------------------------------------------
    class ScoresPipeline < BaseReport
      def initialize(reportable)
        super
        @max_q_score = precompute_max_q_scores
      end

      def source
        Response
          .submitted_review_responses_for(@reportable.id)
          .includes(:response_map, scores: :item)
      end
      def grouper       = ->(r) { r.response_map.reviewer_id }
      def initial_state = Hash.new { |h, k| h[k] = Hash.new { |h2, k2| h2[k2] = {} } }

      def accumulate(state, reviewer_id, response)
        answered  = response.scores.reject { |s| s.answer.nil? }
        total_wt  = answered.sum { |s| s.item.weight }
        return if total_wt.zero?

        round       = response.round || 1
        reviewee_id = response.response_map.reviewee_id
        raw_score   = answered.sum { |s| s.answer * s.item.weight }
        max_score   = total_wt * (@max_q_score[round] || @max_q_score[nil] || 1)
        return unless max_score > 0

        state[reviewer_id][round][reviewee_id] = ((raw_score.to_f / max_score) * 100).round(2)
      end

      private

      def precompute_max_q_scores
        AssignmentQuestionnaire
          .joins(:questionnaire)
          .where(assignment_id: @reportable.id)
          .pluck(:used_in_round, 'questionnaires.max_question_score')
          .to_h
      end
    end

    # -----------------------------------------------------------------------
    # Pipeline 3 — per-team × round aggregate (max / min / avg).
    # Same response stream as Pipeline 2; groups by (reviewee_id, round).
    # Output: { team_id => { round => { max:, min:, avg: } } }
    # -----------------------------------------------------------------------
    class AvgRangesPipeline < BaseReport
      def initialize(reportable)
        super
        @max_q_score = precompute_max_q_scores
      end

      def source
        Response
          .submitted_review_responses_for(@reportable.id)
          .includes(:response_map, scores: :item)
      end

      def grouper       = ->(r) { [r.response_map.reviewee_id, r.round || 1] }
      def initial_state = Hash.new { |h, k| h[k] = [] }

      def accumulate(state, key, response)
        answered  = response.scores.reject { |s| s.answer.nil? }
        total_wt  = answered.sum { |s| s.item.weight }
        return if total_wt.zero?

        round     = key[1]
        raw_score = answered.sum { |s| s.answer * s.item.weight }
        max_score = total_wt * (@max_q_score[round] || @max_q_score[nil] || 1)
        return unless max_score > 0

        state[key] << ((raw_score.to_f / max_score) * 100)
      end

      def finalize(state)
        result = {}
        state.each do |(team_id, round), scores|
          result[team_id]       ||= {}
          result[team_id][round]  = {
            max: scores.max.round(2),
            min: scores.min.round(2),
            avg: (scores.sum / scores.size).round(2)
          }
        end
        result
      end

      private

      def precompute_max_q_scores
        AssignmentQuestionnaire
          .joins(:questionnaire)
          .where(assignment_id: @reportable.id)
          .pluck(:used_in_round, 'questionnaires.max_question_score')
          .to_h
      end
    end
  end
end
