# frozen_string_literal: true

module Reports
  # Teammate-review report: for each reviewer, shows how many teammates they
  # have in total, how many they reviewed, and the individual reviewee details.
  #
  # Two reducers write into a single shared state keyed by user_id:
  #
  #   TeammateReviewReducer — streams TeammateReviewResponseMap rows to populate
  #     reviewer identity fields and build the reviewees list.
  #     Writes to state: { user_id => { ..., reviewed_count:, reviewees: [] } }
  #
  #   TeamSizeReducer — streams TeamsUser rows for the assignment to set
  #     teammate_count, user_id, and name for all team members (including
  #     those who have not done any review).
  #     Writes to state: { user_id => { ..., user_id:, name:, teammate_count: } }
  #
  # Shared state shape (keyed by user_id):
  #   { user_id => { reviewer_id:,                         ← filled by TeammateReviewReducer (nil if no review)
  #                  user_id:, name:,                      ← filled by both (TeamSizeReducer & TeammateReviewReducer)
  #                  teammate_count:,                       ← filled by TeamSizeReducer
  #                  reviewed_count:,                       ← filled by TeammateReviewReducer
  #                  reviewees: [{ reviewee_id:, name: }] } ← filled by TeammateReviewReducer }}
  class TeammateReviewReport
    def self.for_assignment(assignment)
      new(assignment)
    end

    def initialize(reportable)
      @reportable = reportable
    end

    # Runs both reducers and returns:
    #   { reviewers: [{ reviewer_id:, user_id:, name:, teammate_count:,
    #                   reviewed_count:, reviewees: [...] }, ...] }
    def run
      shared_state = Hash.new do |state, user_id|
        state[user_id] = {
          reviewer_id:    nil,
          user_id:        nil,
          name:           nil,
          teammate_count: 0,
          reviewed_count: 0,
          reviewees:      []
        }
      end
      TeammateReviewReducer.new(@reportable).run(shared_state)
      TeamSizeReducer.new(@reportable).run(shared_state)
      { reviewers: shared_state.values }
    end

    # -----------------------------------------------------------------------
    # Reducer 1 — per-reviewer reviewee details and reviewed count.
    # Streams TeammateReviewResponseMap rows; on first occurrence per reviewer
    # populates identity fields, then appends each reviewee to the list.
    # -----------------------------------------------------------------------
    class TeammateReviewReducer < BaseReport
      def source
        TeammateReviewResponseMap
          .for_assignment(@reportable.id)
          .includes(reviewer: :user, reviewee: :user)
      end

      def initial_state = {}

      def accumulate(state, map)
        reviewer = map.reviewer
        return unless reviewer

        user_id = reviewer.user_id

        unless state.key?(user_id)
          state[user_id][:reviewer_id] = reviewer.id
          state[user_id][:user_id]     = reviewer.user_id
          state[user_id][:name]        = reviewer.user&.name
        end

        reviewee = map.reviewee
        return unless reviewee

        state[user_id][:reviewees] << {
          reviewee_id: reviewee.id,
          name:        reviewee.user&.name
        }
        state[user_id][:reviewed_count] += 1
      end
    end

    # -----------------------------------------------------------------------
    # Reducer 2 — per-team-member teammate count.
    # Streams TeamsUser rows for the assignment. Increments teammate_count
    # for every team member, including those who have not done any review.
    # teammate_count counts all TeamsUser rows for the user's team,
    # including themselves.
    # -----------------------------------------------------------------------
    class TeamSizeReducer < BaseReport
      def source
        TeamsUser
          .joins(:team)
          .where(teams: { parent_id: @reportable.id, type: 'AssignmentTeam' })
          .includes(:user)
      end

      def initial_state = {}

      def accumulate(state, teams_user)
        user_id = teams_user.user_id
        state[user_id][:user_id] ||= user_id
        state[user_id][:name]    ||= teams_user.user&.name
        state[user_id][:teammate_count] += 1
      end
    end
  end
end
