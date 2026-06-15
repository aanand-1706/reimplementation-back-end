# frozen_string_literal: true

module Reports
  # Peer-review report: returns raw review rows (reviewer, reviewee, responses,
  # scores) plus computed score percentages per reviewer and per-team averages.
  #
  # Three data sources:
  #
  #   Review rows — loads all ReviewResponseMaps for the assignment with reviewer,
  #     reviewee, responses, and scores eagerly loaded, then serializes via as_json.
  #
  #   ScoresReducer — streams submitted Response rows (one per reviewer × reviewee ×
  #     round) and computes score percentage from the response's answers relative to
  #     the questionnaire's max score.
  #     Output: { reviewer_id => { reviewee_id => { round => score_pct } } }
  #
  #   AvgRangesReducer — streams AssignmentTeam rows. For each team, the average
  #     review score is computed from its eagerly-loaded review_mappings and their
  #     nested responses and scores (via aggregate_review_grade).
  #     Output: { team_id => avg_score }
  #
  # Output:
  #   {
  #     reviews: [ { id:,
  #                  reviewer: { id:, user: { id:, name: } },
  #                  reviewee: { id:, user: { id:, name: } },
  #                  responses: [ { id:, round:, is_submitted:, additional_comment:,
  #                                 scores: [ { id:, answer:, comments:,
  #                                             item: { id:, txt:, weight: } } ] } ]
  #                }, ... ],
  #     scores:         { reviewer_id => { reviewee_id => { round => score_pct } } },
  #     avg_and_ranges: { team_id => avg_score }
  #   }
  class ReviewReport
    # Whitelist for as_json — includes only fields the frontend needs,
    # excluding internal columns (raw FKs, timestamps, STI type, etc.).
    MAP_JSON_OPTIONS = {
      only: [:id],
      include: {
        reviewer: {
          only: [:id],
          include: { user: { only: %i[id name] } }
        },
        reviewee: {
          only: [:id],
          include: { user: { only: %i[id name] } }
        },
        responses: {
          only: %i[id round additional_comment is_submitted],
          include: {
            scores: {
              only: %i[id answer comments],
              include: { item: { only: %i[id txt weight] } }
            }
          }
        }
      }
    }.freeze

    def self.for_assignment(assignment)
      new(assignment)
    end

    def initialize(reportable)
      @reportable = reportable
    end

    def run
      maps = ReviewResponseMap
               .for_assignment(@reportable.id)
               .includes(reviewer: :user, reviewee: :user, responses: { scores: :item })

      {
        reviews:        maps.as_json(MAP_JSON_OPTIONS),
        scores:         ScoresReducer.new(@reportable).run,
        avg_and_ranges: AvgRangesReducer.new(@reportable).run
      }
    end

    # -----------------------------------------------------------------------
    # Reducer 1 — per-reviewer × reviewee × round score percentages.
    # Streams submitted Responses with scores and questionnaires eagerly loaded
    # to avoid N+1 inside calculate_total_score and maximum_score.
    # Output: { reviewer_id => { reviewee_id => { round => score_pct } } }
    # -----------------------------------------------------------------------
    class ScoresReducer < BaseReducer
      def source
        Response
          .submitted_review_responses_for(@reportable.id)
          .includes(:response_map, scores: { item: :questionnaire })
      end

      def initial_state
        Hash.new do |state, reviewer_id|
          state[reviewer_id] = Hash.new { |by_reviewee, reviewee_id| by_reviewee[reviewee_id] = {} }
        end
      end

      def accumulate(state, response)
        return if response.maximum_score.zero?

        reviewer_id = response.response_map.reviewer_id
        reviewee_id = response.response_map.reviewee_id
        round       = response.round || 1
        score_pct   = (response.aggregate_questionnaire_score.to_f / response.maximum_score * 100).round(2)

        state[reviewer_id][reviewee_id][round] = score_pct
      end
    end

    # -----------------------------------------------------------------------
    # Reducer 2 — per-team average review score.
    # Streams AssignmentTeam rows with review_mappings, responses, and scores
    # eagerly loaded to avoid N+1. Delegates score computation to
    # aggregate_review_grade which picks the latest submitted response per
    # round per map and normalizes the score.
    # Output: { team_id => avg_score }
    # -----------------------------------------------------------------------
    class AvgRangesReducer < BaseReducer
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