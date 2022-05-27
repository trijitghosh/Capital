-- The staging table
create table stg_trials(data json, file_name varchar, update_date date);

-- The table where the required data is stored
create table clinic_trials(
    NCTNumber varchar primary key,
    Status varchar,
    StudyTitle varchar,
    Condition json,
    StudyType varchar,
    Phase json,
    InterventionType varchar,
    InterventionName varchar,
    LeadSponsorName varchar,
    Funder_Type varchar,
    CollaboratorName varchar,
    StudyDesign json,
    PrimaryOutcome json,
    SecondaryOutcome json,
    NumberEnrolled int,
    sex varchar,
    age varchar,
    OtherIDs varchar,
    TitleAcronym json,
    StudyStart varchar,
    PrimaryCompletion varchar,
    StudyCompletion varchar,
    FirstPosted varchar,
    LastUpdatePosted varchar,
    ResultsFirstPosted varchar,
    Locations json,
    StudyDocuments json,
    update_date date
);