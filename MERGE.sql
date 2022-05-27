-- The query to merge data from the stg table to the actual table
-- This makes sure that no duplicate data in inserted into the table
with json_data as (select data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'IdentificationModule' ->>
                          'NCTId'                                                   as NCTNumber,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->>
                          'OverallStatus'                                           as Status,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'IdentificationModule' ->>
                          'BriefTitle'                                              as StudyTitle,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'ConditionsModule' ->
                          'ConditionList' ->
                          'Condition'                                               as Condition,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'DesignModule' ->>
                          'StudyType'                                               as StudyType,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'DesignModule' -> 'PhaseList' ->
                          'Phase'                                                   as Phase,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'ArmsInterventionsModule' ->
                          'InterventionList' ->
                          'Intervention' ->
                          'InterventionType'                                        as InterventionType,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'ArmsInterventionsModule' ->
                          'InterventionList' ->
                          'Intervention' ->
                          'InterventionName'                                        as InterventionName,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'SponsorCollaboratorsModule' ->
                          'LeadSponsor' ->>
                          'LeadSponsorName'                                         as LeadSponsorName,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'SponsorCollaboratorsModule' ->
                          'LeadSponsor' ->>
                          'LeadSponsorClass'                                        as Funder_Type,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'SponsorCollaboratorsModule' ->
                          'CollaboratorList' ->
                          'Collaborator'                                            as CollaboratorName,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'DesignModule' ->
                          'DesignInfo'                                              as StudyDesign,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'OutcomesModule' ->
                          'PrimaryOutcomeList' ->
                          'PrimaryOutcome'                                          as PrimaryOutcome,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'OutcomesModule' ->
                          'SecondaryOutcomeList' ->
                          'SecondaryOutcome'                                        as SecondaryOutcome,
                          cast(data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'DesignModule' ->
                               'EnrollmentInfo' ->>
                               'EnrollmentCount' as int)                            as NumberEnrolled,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'EligibilityModule' ->>
                          'Gender'                                                  as sex,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'EligibilityModule' ->>
                          'MinimumAge'                                              as age,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'IdentificationModule' ->
                          'OrgStudyIdInfo' ->>
                          'OrgStudyId'                                              as OtherIDs,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'IdentificationModule' ->
                          'Acronym'                                                 as TitleAcronym,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'StartDateStruct' ->>
                          'StartDate'                                               as StudyStart,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'PrimaryCompletionDateStruct' ->> 'PrimaryCompletionDate' as PrimaryCompletion,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'CompletionDateStruct' ->>
                          'CompletionDate'                                          as StudyCompletion,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'StudyFirstPostDateStruct' ->>
                          'StudyFirstPostDate'                                      as FirstPosted,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'LastUpdatePostDateStruct' ->>
                          'LastUpdatePostDate'                                      as LastUpdatePosted,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'StatusModule' ->
                          'ResultsFirstPostDateStruct' ->>
                          'ResultsFirstPostDate'                                    as ResultsFirstPosted,
                          data -> 'FullStudy' -> 'Study' -> 'ProtocolSection' -> 'ContactsLocationsModule' ->
                          'LocationList' ->
                          'Location'                                                as Locations,
                          data -> 'FullStudy' -> 'Study' -> 'DocumentSection' -> 'LargeDocumentModule' ->
                          'LargeDocList' ->
                          'LargeDoc'                                                as StudyDocuments,
                          update_date
                   from stg_trials)
insert
into clinic_trials (NCTNumber,
                    Status,
                    StudyTitle,
                    Condition,
                    StudyType,
                    Phase,
                    InterventionType,
                    InterventionName,
                    LeadSponsorName,
                    Funder_Type,
                    CollaboratorName,
                    StudyDesign,
                    PrimaryOutcome,
                    SecondaryOutcome,
                    NumberEnrolled,
                    sex,
                    age,
                    OtherIDs,
                    TitleAcronym,
                    StudyStart,
                    PrimaryCompletion,
                    StudyCompletion,
                    FirstPosted,
                    LastUpdatePosted,
                    ResultsFirstPosted,
                    Locations,
                    StudyDocuments,
                    update_date)
select NCTNumber,
       Status,
       StudyTitle,
       Condition,
       StudyType,
       Phase,
       InterventionType,
       InterventionName,
       LeadSponsorName,
       Funder_Type,
       CollaboratorName,
       StudyDesign,
       PrimaryOutcome,
       SecondaryOutcome,
       NumberEnrolled,
       sex,
       age,
       OtherIDs,
       TitleAcronym,
       StudyStart,
       PrimaryCompletion,
       StudyCompletion,
       FirstPosted,
       LastUpdatePosted,
       ResultsFirstPosted,
       Locations,
       StudyDocuments,
       update_date
from json_data
on conflict(NCTNumber)
    do update set NCTNumber=coalesce(excluded.NCTNumber, clinic_trials.NCTNumber),
                  Status=coalesce(excluded.Status, clinic_trials.Status),
                  StudyTitle=coalesce(excluded.StudyTitle, clinic_trials.StudyTitle),
                  Condition=coalesce(excluded.Condition, clinic_trials.Condition),
                  StudyType=coalesce(excluded.StudyType, clinic_trials.StudyType),
                  Phase=coalesce(excluded.Phase, clinic_trials.Phase),
                  InterventionType=coalesce(excluded.InterventionType, clinic_trials.InterventionType),
                  InterventionName=coalesce(excluded.InterventionName, clinic_trials.InterventionName),
                  LeadSponsorName=coalesce(excluded.LeadSponsorName, clinic_trials.LeadSponsorName),
                  Funder_Type=coalesce(excluded.Funder_Type, clinic_trials.Funder_Type),
                  CollaboratorName=coalesce(excluded.CollaboratorName, clinic_trials.CollaboratorName),
                  StudyDesign=coalesce(excluded.StudyDesign, clinic_trials.StudyDesign),
                  PrimaryOutcome=coalesce(excluded.PrimaryOutcome, clinic_trials.PrimaryOutcome),
                  SecondaryOutcome=coalesce(excluded.SecondaryOutcome, clinic_trials.SecondaryOutcome),
                  NumberEnrolled=coalesce(excluded.NumberEnrolled, clinic_trials.NumberEnrolled),
                  sex=coalesce(excluded.sex, clinic_trials.sex),
                  age=coalesce(excluded.age, clinic_trials.age),
                  OtherIDs=coalesce(excluded.OtherIDs, clinic_trials.OtherIDs),
                  TitleAcronym=coalesce(excluded.TitleAcronym, clinic_trials.TitleAcronym),
                  StudyStart=coalesce(excluded.StudyStart, clinic_trials.StudyStart),
                  PrimaryCompletion=coalesce(excluded.PrimaryCompletion, clinic_trials.PrimaryCompletion),
                  StudyCompletion=coalesce(excluded.StudyCompletion, clinic_trials.StudyCompletion),
                  FirstPosted=coalesce(excluded.FirstPosted, clinic_trials.FirstPosted),
                  LastUpdatePosted=coalesce(excluded.LastUpdatePosted, clinic_trials.LastUpdatePosted),
                  ResultsFirstPosted=coalesce(excluded.ResultsFirstPosted, clinic_trials.ResultsFirstPosted),
                  Locations=coalesce(excluded.Locations, clinic_trials.Locations),
                  StudyDocuments=coalesce(excluded.StudyDocuments, clinic_trials.StudyDocuments),
                  update_date=coalesce(excluded.update_date, clinic_trials.update_date);