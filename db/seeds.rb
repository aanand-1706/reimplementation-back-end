# frozen_string_literal: true

begin
  # Create an instritution
  inst_id = Institution.create!(
    name: 'North Carolina State University'
  ).id

  Role.create!(id: 1, name: 'Super Administrator')
  Role.create!(id: 2, name: 'Administrator')
  Role.create!(id: 3, name: 'Instructor')
  Role.create!(id: 4, name: 'Teaching Assistant')
  Role.create!(id: 5, name: 'Student')

  # Create an admin user
  User.create!(
    name: 'admin',
    email: 'admin2@example.com',
    password: 'password123',
    full_name: 'admin admin',
    institution_id: 1,
    role_id: 1
  )

  # Generate Random Users
  num_students = 48
  num_assignments = 8
  num_teams = 16
  num_courses = 2
  num_instructors = 2

  puts "creating instructors"
  instructor_user_ids = []
  num_instructors.times do
    instructor_user_ids << User.create(
      name: Faker::Internet.unique.username,
      email: Faker::Internet.unique.email,
      password: "password",
      full_name: Faker::Name.name,
      institution_id: 1,
      role_id: 3
    ).id
  end

  puts "creating courses"
  course_ids = []
  num_courses.times do |i|
    course_ids << Course.create(
      instructor_id: instructor_user_ids[i],
      institution_id: inst_id,
      directory_path: Faker::File.dir(segment_count: 2),
      name: Faker::Company.industry,
      info: "A fake class",
      private: false
    ).id
  end

  puts "creating assignments"
  assignment_ids = []
  num_assignments.times do |i|
    assignment_ids << Assignment.create(
      name: Faker::Verb.base,
      instructor_id: instructor_user_ids[i % num_instructors],
      course_id: course_ids[i % num_courses],
      has_teams: true,
      private: false
    ).id
  end

  puts "creating teams"
  team_ids = []
  num_teams.times do |i|
    team_ids << AssignmentTeam.create(
      name: "Team #{i + 1}",
      parent_id: assignment_ids[i % num_assignments]
    ).id
  end

  puts "creating students"
  student_user_ids = []
  num_students.times do
    student_user_ids << User.create(
      name: Faker::Internet.unique.username,
      email: Faker::Internet.unique.email,
      password: "password",
      full_name: Faker::Name.name,
      institution_id: 1,
      role_id: 5,
      parent_id: [nil, *instructor_user_ids].sample
    ).id
  end

  puts "assigning students to teams"
  teams_users_ids = []
  # num_students.times do |i|
  #  teams_users_ids << TeamsUser.create(
  #    team_id: team_ids[i%num_teams],
  #    user_id: student_user_ids[i]
  #  ).id
  # end

  num_students.times do |i|
    puts "Creating TeamsUser with team_id: #{team_ids[i % num_teams]}, user_id: #{student_user_ids[i]}"
    teams_user = TeamsUser.create(
      team_id: team_ids[i % num_teams],
      user_id: student_user_ids[i]
    )
    if teams_user.persisted?
      teams_users_ids << teams_user.id
      puts "Created TeamsUser with ID: #{teams_user.id}"
    else
      puts "Failed to create TeamsUser: #{teams_user.errors.full_messages.join(', ')}"
    end
  end

  puts "assigning participant to students, teams, courses, and assignments"
  participant_ids = []
  num_students.times do |i|
    participant = AssignmentParticipant.create(
      user_id: student_user_ids[i],
      parent_id: assignment_ids[i%num_assignments],
      team_id: team_ids[i%num_teams],
      handle: Faker::Internet.unique.username,
    )
    if participant.persisted?
      puts "Created AssignmentParticipant with ID: #{participant.id}"
      participant_ids << participant.id
      TeamsParticipant.create!(
        team_id: team_ids[i%num_teams],
        participant_id: participant.id,
        user_id: student_user_ids[i]
      )
    else
      puts "Failed to create AssignmentParticipant: #{participant.errors.full_messages.join(', ')}"
    end
  end

rescue ActiveRecord::RecordInvalid => e
  puts e, 'The db has already been seeded'
end

# ---------------------------------------------------------------------------
# Review report seed data
# Creates the questionnaire, questions, response maps, responses, and answers
# needed to exercise the ReviewReport for the first assignment.
# ---------------------------------------------------------------------------
begin
  assignment   = Assignment.first
  instructor   = User.find_by(role_id: 3)

  puts "creating review questionnaire"
  questionnaire = Questionnaire.create!(
    name: 'Peer Review Rubric',
    instructor_id: instructor.id,
    private: false,
    min_question_score: 0,
    max_question_score: 5,
    questionnaire_type: 'ReviewQuestionnaire'
  )

  puts "creating questionnaire items"
  items = [
    'How well did the team communicate?',
    'How thoroughly were the deliverables completed?',
    'Rate the overall quality of the work.'
  ].each_with_index.map do |txt, i|
    Item.create!(
      questionnaire_id: questionnaire.id,
      txt: txt,
      weight: 1,
      seq: i + 1,
      question_type: 'Criterion',
      break_before: true
    )
  end

  puts "linking questionnaire to assignment"
  AssignmentQuestionnaire.find_or_create_by!(
    assignment_id: assignment.id,
    questionnaire_id: questionnaire.id,
    used_in_round: 1
  ) { |aq| aq.questionnaire_weight = 100; aq.notification_limit = 15 }

  AssignmentQuestionnaire.find_or_create_by!(
    assignment_id: assignment.id,
    questionnaire_id: questionnaire.id,
    used_in_round: 2
  ) { |aq| aq.questionnaire_weight = 100; aq.notification_limit = 15 }

  teams        = AssignmentTeam.where(parent_id: assignment.id).to_a
  participants = AssignmentParticipant.where(parent_id: assignment.id).to_a

  puts "creating review response maps, responses, and answers"
  participants.each_with_index do |reviewer, idx|
    reviewee_team = teams.reject { |t| t.id == reviewer.team_id }.first
    next if reviewee_team.nil?

    # One map per reviewer-reviewee pair — idempotent
    map = ReviewResponseMap.find_or_create_by!(
      reviewed_object_id: assignment.id,
      reviewer_id: reviewer.id,
      reviewee_id: reviewee_team.id
    )

    # Round 1 — all reviewers
    r1 = Response.find_or_create_by!(map_id: map.id, round: 1) do |r|
      r.is_submitted = true
      r.additional_comment = Faker::Lorem.sentence
    end
    items.each do |item|
      Answer.find_or_create_by!(response_id: r1.id, item_id: item.id) do |a|
        a.answer   = rand(3..5)
        a.comments = Faker::Lorem.sentence
      end
    end

    # Round 2 — first reviewer only, so scores vs avg_and_ranges differ visibly
    next unless idx.zero?

    r2 = Response.find_or_create_by!(map_id: map.id, round: 2) do |r|
      r.is_submitted = true
      r.additional_comment = Faker::Lorem.sentence
    end
    items.each do |item|
      Answer.find_or_create_by!(response_id: r2.id, item_id: item.id) do |a|
        a.answer   = rand(1..3)
        a.comments = Faker::Lorem.sentence
      end
    end
  end

  puts "Review report seed data created successfully."
rescue ActiveRecord::RecordInvalid => e
  puts e, 'Review report seed data may already exist.'
end
