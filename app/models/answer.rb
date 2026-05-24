# frozen_string_literal: true

class Answer < ApplicationRecord
    belongs_to :response
    belongs_to :item

  scope :for_items_and_responses, ->(item_ids, response_ids) { where(item_id: item_ids, response_id: response_ids) }
end
