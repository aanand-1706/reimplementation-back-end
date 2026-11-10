# frozen_string_literal: true

class UserSerializer < ActiveModel::Serializer
  attributes :id, :username, :email, :name

  def username
    object.name
  end

  def name
    object.full_name
  end
end
