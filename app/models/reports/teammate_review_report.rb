# frozen_string_literal: true

module Reports
  # Teammate-review report: returns raw review rows (reviewer, reviewee,
  # responses, scores) plus a per-user teammate count derived from team
  # membership — the one aggregate that cannot come from the review rows alone.
  #
  # Two data sources:
  #
  #   Review rows — a single AR query on TeammateReviewResponseMap with all
  #     associations eagerly loaded, serialized via as_json. Each row contains
  #     reviewer, reviewee, and full response/score content. The frontend can
  #     derive reviewed_count by counting rows where reviewer.user.id matches.
  #
  #   TeamSizeReducer — streams AssignmentTeam rows with teams_users eagerly
  #     loaded. For each team, distributes (team_size - 1) to every member's
  #     teammate_count, excluding themselves. Sums correctly if a user is on
  #     more than one team. Users with no reviews still appear here.
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
  #     teammate_counts: { user_id => count }
  #   }
  class TeammateReviewReport
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
      maps = TeammateReviewResponseMap
               .for_assignment(@reportable.id)
               .includes(reviewer: :user, reviewee: :user, responses: { scores: :item })

      {
        reviews:         maps.as_json(MAP_JSON_OPTIONS),
        teammate_counts: TeamSizeReducer.new(@reportable).run
      }
    end

    # -----------------------------------------------------------------------
    # Reducer — per-team-member teammate count.
    # Streams AssignmentTeam rows with teams_users eagerly loaded. For each
    # team, distributes (team_size - 1) to every member's count, excluding
    # themselves. Sums correctly if a user is on more than one team.
    # Output: { user_id => count }
    # -----------------------------------------------------------------------
    class TeamSizeReducer < BaseReducer
      def source
        AssignmentTeam
          .where(parent_id: @reportable.id)
          .includes(:teams_users)
      end

      def initial_state
        Hash.new(0)
      end

      def accumulate(state, team)
        team_size = team.teams_users.size
        team.teams_users.each do |teams_user|
          state[teams_user.user_id] += team_size - 1
        end
      end
    end
  end
end
