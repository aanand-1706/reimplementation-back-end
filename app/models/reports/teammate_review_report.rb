# frozen_string_literal: true

module Reports
  # Teammate-review report: lists reviewers with all reviewees they evaluated
  # for the assignment.
  #
  # Accumulator: groups TeammateReviewResponseMap rows by reviewer_id. On first
  # occurrence, initializes the reviewer entry with an empty reviewees list. On
  # every occurrence, appends the reviewee to that list.
  # reviewer and reviewee associations are eagerly loaded to avoid N+1 queries.
  class TeammateReviewReport < BaseReport
    def source
      TeammateReviewResponseMap
        .where(reviewed_object_id: @reportable.id)
        .includes(reviewer: :user, reviewee: :user)
    end

    def state_key_for = ->(map) { map.reviewer_id }

    def initial_state = {}

    def accumulate(state, reviewer_id, map)
      reviewer = map.reviewer
      return unless reviewer

      state[reviewer_id] ||= {
        reviewer_id: reviewer.id,
        user_id:     reviewer.user_id,
        name:        reviewer.user&.name,
        full_name:   reviewer.user&.full_name,
        reviewees:   []
      }

      reviewee = map.reviewee
      return unless reviewee

      state[reviewer_id][:reviewees] << {
        reviewee_id: reviewee.id,
        name:        reviewee.user&.name,
        full_name:   reviewee.user&.full_name
      }
    end

    def finalize(state)
      { reviewers: state.values }
    end
  end
end
