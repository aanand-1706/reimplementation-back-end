# frozen_string_literal: true

module Reports
  # Teammate-review report: lists distinct reviewers who submitted teammate
  # evaluations for the assignment.
  #
  # Accumulator: groups TeammateReviewResponseMap rows by reviewer_id, keeping
  # one entry per reviewer.  reviewer associations are eagerly loaded to avoid
  # N+1 queries.
  class TeammateReviewReport < BaseReport
    def source
      TeammateReviewResponseMap
        .where(reviewed_object_id: @reportable.id)
        .includes(reviewer: :user)
    end

    def state_key_for       = ->(map) { map.reviewer_id }
    def initial_state = {}

    def accumulate(state, reviewer_id, map)
      return if state.key?(reviewer_id)

      participant = map.reviewer
      return unless participant

      state[reviewer_id] = {
        reviewer_id: participant.id,
        user_id:     participant.user_id,
        name:        participant.user&.name,
        full_name:   participant.user&.full_name
      }
    end

    def finalize(state)
      { reviewers: state.values }
    end
  end
end
