# frozen_string_literal: true

module Reports
  # Answer-tagging report: shows per-user tagging progress for each
  # TagPromptDeployment on the assignment, plus a cross-deployment summary.
  #
  # Output shape:
  #   {
  #     questionnaire_tagging_report: {
  #       <deployment_id> => {
  #         questionnaire_name:, prompt:, question_type:,
  #         answer_length_threshold:,
  #         user_stats: [{ user_id:, name:, full_name:, percentage:,
  #                        cnt_tagged:, cnt_not_tagged:, cnt_taggable:,
  #                        tag_update_intervals: }]
  #       }
  #     },
  #     user_tagging_report: {
  #       "<username>" => { user_id:, name:, full_name:, percentage:,
  #                         cnt_tagged:, cnt_not_tagged:, cnt_taggable: }
  #     }
  #   }
  class AnswerTaggingReport
    def self.for_assignment(assignment)
      new(assignment)
    end

    def initialize(assignment)
      @reportable = assignment
    end

    def run
      per_deployment = {}
      # The coordinator runs one DeploymentPipeline per TagPromptDeployment,
      # then passes the results to UserSummaryPipeline for cross-deployment totals.
      #
      #   DeploymentPipeline (< BaseReport) — streams TeamsUser grouped by user_id:
      #     precomputes answer_ids_by_team (one SQL query) and tags_by_user (one
      #     SQL query), then accumulates per-user tagged / not-tagged / interval
      #     stats with no DB queries inside the hot loop.
      TagPromptDeployment
        .where(assignment_id: @reportable.id)
        .includes(:tag_prompt, :questionnaire)
        .each do |deployment|
          per_deployment[deployment.id] = DeploymentPipeline.new(@reportable, deployment).run
        end
      #   UserSummaryPipeline — aggregates DeploymentPipeline output across
      #     all deployments into a single per-user total. No additional DB queries.
      user_summary = UserSummaryPipeline.new(per_deployment).run
      {
        questionnaire_tagging_report: per_deployment,
        user_tagging_report: user_summary
      }
    end

    # -------------------------------------------------------------------------
    # Pipeline 1 — per-user tagging stats for one TagPromptDeployment.
    # Streams TeamsUser (one row per assignment member) grouped by user_id.
    # All DB-heavy work (answer IDs, tags) is precomputed before the hot loop.
    # -------------------------------------------------------------------------
    class DeploymentPipeline < BaseReport
      def initialize(reportable, deployment)
        super(reportable)
        @deployment = deployment
        @tags_by_user = AnswerTag.for_deployment(deployment.id).group_by(&:user_id)
        @taggable_answer_ids_by_team = fetch_taggable_answer_ids_associated_with_deployment
      end

      def source
        # For looping through users present in teams of the assignment.
        TeamsUser.for_assignment(@reportable.id).includes(:user)
      end

      def grouper = ->(team_membership) { team_membership.user_id }

      def initial_state = {}

      # Will be called for each row in the source stream (i.e., per each team user)
      def accumulate(state, user_id, team_membership)
        answer_ids = @taggable_answer_ids_by_team[team_membership.team_id] || []

        # Assuming that a user is only a member of one team. Check with prof??
        cnt_taggable = answer_ids.size
        return if cnt_taggable.zero?

        user_tags = (@tags_by_user[user_id] || []).select { |tag| answer_ids.include?(tag.answer_id) }
        cnt_tagged = user_tags.size
        cnt_not_tagged = cnt_taggable - cnt_tagged

        tag_times_in_order = user_tags.map(&:updated_at).sort
        # fetches 2 consecutive timestamps and computes the difference
        intervals_between_tags = tag_times_in_order.each_cons(2).map do |earlier, later|
          later - earlier
        end

        state[user_id] = {
          user_id: user_id,
          name: team_membership.user.name,
          full_name: team_membership.user.full_name,
          percentage: format('%.1f', cnt_tagged.to_f / cnt_taggable * 100),
          cnt_tagged: cnt_tagged,
          cnt_not_tagged: cnt_not_tagged,
          cnt_taggable: cnt_taggable,
          tag_update_intervals: intervals_between_tags
        }
      end

      def finalize(state)
        {
          questionnaire_name: @deployment.questionnaire.name,
          prompt: @deployment.tag_prompt.prompt,
          question_type: @deployment.question_type,
          answer_length_threshold: @deployment.answer_length_threshold,
          user_stats: state.values
        }
      end

      private

      # One SQL query that returns taggable answer IDs keyed by team_id.
      # No DB calls in the hot loop.
      def fetch_taggable_answer_ids_associated_with_deployment
        item_ids = fetch_item_ids_associated_with_deployment_questionnaire
        return {} if item_ids.empty?

        response_ids_by_team = fetch_responses_received_by_teams_across_questionnaires_for_deployment_assignment

        # Transforms { team_id => [response_ids] } into { team_id => [answer_ids] }
        response_ids_by_team.transform_values do |response_ids|
          scope = Answer.for_items_and_responses(item_ids, response_ids)
          scope = scope.where('LENGTH(comments) > ?', @deployment.answer_length_threshold) if @deployment.answer_length_threshold
          scope.pluck(:id)
        end
      end

      def fetch_item_ids_associated_with_deployment_questionnaire
        Item.for_questionnaire_and_type(@deployment.questionnaire_id, @deployment.question_type).pluck(:id)
      end

      # Returns { team_id => [response_id, ...] } across all rubrics
      # on this assignment. Scoping to the deployment's questionnaire happens
      # via item_ids in fetch_taggable_answer_ids_by_team, not here.
      def fetch_responses_received_by_teams_across_questionnaires_for_deployment_assignment
        team_response_pairs = Response
          .submitted_review_responses_for(@reportable.id)
          .pluck('response_maps.reviewee_id', 'responses.id')

        response_ids_by_team = Hash.new { |by_team, team_id| by_team[team_id] = [] }
        team_response_pairs.each do |team_id, response_id|
          response_ids_by_team[team_id] << response_id
        end
        response_ids_by_team
      end
    end

    # -------------------------------------------------------------------------
    # Pipeline 2 — cross-deployment per-user summary.
    # Consumes DeploymentPipeline output; no additional DB queries.
    # -------------------------------------------------------------------------
    class UserSummaryPipeline
      def initialize(per_deployment_result)
        @per_deployment = per_deployment_result
      end

      def run
        summary = {}
        @per_deployment.each_value do |deployment_data|
          deployment_data[:user_stats].each do |stat|
            key = stat[:name]
            if summary.key?(key)
              entry = summary[key]
              entry[:cnt_tagged] += stat[:cnt_tagged]
              entry[:cnt_not_tagged] += stat[:cnt_not_tagged]
              entry[:cnt_taggable] += stat[:cnt_taggable]
              entry[:percentage] = entry[:cnt_taggable].zero? ? '-' : format('%.1f', entry[:cnt_tagged].to_f / entry[:cnt_taggable] * 100)
            else
              summary[key] = stat.slice(:user_id, :name, :full_name, :cnt_tagged, :cnt_not_tagged, :cnt_taggable, :percentage)
            end
          end
        end
        summary
      end
    end
  end
end
