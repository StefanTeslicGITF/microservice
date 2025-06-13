package com.globalitfactory.api.v1.service.rabbitMQ;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CurriculumVersionDTO implements Serializable {

    private Integer id;
    private Integer lastUpdateId;
    private Integer curriculumId;
    private Integer curriculumVersionId;

    private String typeName;
    private String typeNameLc;
    private String defaultType;

    private String subtypeName;
    private String subtypeNameLc;

    private String versionedCode;
    private String versionedCodeLc;

    private String versionedName;
    private String versionedNameLc;

    private String description;
    private String descriptionLc;

    private String abbreviation;
    private String abbreviationLc;

    private Integer version;
    private Integer versionMinor;

    private Integer fromAcadYear;
    private String fromStudyPeriod;
    private String fromStudyPeriodLc;

    private Integer toAcadYear;
    private String toStudyPeriod;
    private String toStudyPeriodLc;

    private Integer orgUnitId;
    private String orgUnitName;
    private String orgUnitNameLc;

    private String workflowName;
    private String workflowNameLc;

    private String stateName;
    private String stateNameLc;

    private LocalDateTime lastStateChangeDate;
    private LocalDateTime lastUpdateDate;

    private String proposerName;
    private String proposerNameLc;

    private LocalDateTime proposedDate;
    private LocalDateTime approvedDate;

    private Integer unitsMinimum;
    private Integer unitsMaximum;

    private String templateName;
    private String templateNameLc;
    private Integer templateVersion;

    private Boolean latestNonRejected;
    private Boolean latestNonRejectedMinor;

    private String lastUpdateBy;
    private String lastUpdateByLc;

    private String lastStateChangeBy;
    private String lastStateChangeByLc;

    private String primaryCurriculumCode;
    private String primaryCurriculumCodeLc;

    private String primaryCurriculumDetails;
    private String primaryCurriculumDetailsLc;

    private LocalDateTime proposedDatetime;
    private LocalDateTime approvedDatetime;
    private LocalDateTime datetimeLastStateChange;
    private LocalDateTime datetimeLastUpdate;

    private String userCommentDescription;
    private String userCommentDescriptionLc;

    // --- Static factory method ---
    public static CurriculumVersionDTO fromResultSet(ResultSet rs) throws SQLException {
        CurriculumVersionDTO dto = new CurriculumVersionDTO();

        dto.id = rs.getInt("ID");
        dto.lastUpdateId = rs.getInt("LAST_UPDATE_ID");
        dto.curriculumId = rs.getInt("CURRICULUM_ID");
        dto.curriculumVersionId = rs.getInt("CURRICULUM_VERSION_ID");

        dto.typeName = rs.getString("TYPE_NAME");
        dto.typeNameLc = rs.getString("TYPE_NAME_LC");
        dto.defaultType = rs.getString("DEFAULT_TYPE");

        dto.subtypeName = rs.getString("SUBTYPE_NAME");
        dto.subtypeNameLc = rs.getString("SUBTYPE_NAME_LC");

        dto.versionedCode = rs.getString("VERSIONED_CODE");
        dto.versionedCodeLc = rs.getString("VERSIONED_CODE_LC");

        dto.versionedName = rs.getString("VERSIONED_NAME");
        dto.versionedNameLc = rs.getString("VERSIONED_NAME_LC");

        dto.description = rs.getString("DESCRIPTION");
        dto.descriptionLc = rs.getString("DESCRIPTION_LC");

        dto.abbreviation = rs.getString("ABBREVIATION");
        dto.abbreviationLc = rs.getString("ABBREVIATION_LC");

        dto.version = rs.getInt("VERSION");
        dto.versionMinor = rs.getInt("VERSION_MINOR");

        dto.fromAcadYear = rs.getInt("FROM_ACAD_YEAR");
        dto.fromStudyPeriod = rs.getString("FROM_STUDY_PERIOD");
        dto.fromStudyPeriodLc = rs.getString("FROM_STUDY_PERIOD_LC");

        dto.toAcadYear = rs.getInt("TO_ACAD_YEAR");
        dto.toStudyPeriod = rs.getString("TO_STUDY_PERIOD");
        dto.toStudyPeriodLc = rs.getString("TO_STUDY_PERIOD_LC");

        dto.orgUnitId = rs.getInt("ORG_UNIT_ID");
        dto.orgUnitName = rs.getString("ORG_UNIT_NAME");
        dto.orgUnitNameLc = rs.getString("ORG_UNIT_NAME_LC");

        dto.workflowName = rs.getString("WORKFLOW_NAME");
        dto.workflowNameLc = rs.getString("WORKFLOW_NAME_LC");

        dto.stateName = rs.getString("STATE_NAME");
        dto.stateNameLc = rs.getString("STATE_NAME_LC");

        dto.lastStateChangeDate = getLocalDateTime(rs, "LAST_STATE_CHANGE_DATE");
        dto.lastUpdateDate = getLocalDateTime(rs, "LAST_UPDATE_DATE");

        dto.proposerName = rs.getString("PROPOSER_NAME");
        dto.proposerNameLc = rs.getString("PROPOSER_NAME_LC");

        dto.proposedDate = getLocalDateTime(rs, "PROPOSED_DATE");
        dto.approvedDate = getLocalDateTime(rs, "APPROVED_DATE");

        dto.unitsMinimum = rs.getInt("UNITS_MINIMUM");
        dto.unitsMaximum = rs.getInt("UNITS_MAXIMUM");

        dto.templateName = rs.getString("TEMPLATE_NAME");
        dto.templateNameLc = rs.getString("TEMPLATE_NAME_LC");
        dto.templateVersion = rs.getInt("TEMPLATE_VERSION");

        dto.latestNonRejected = rs.getBoolean("LATEST_NON_REJECTED");
        dto.latestNonRejectedMinor = rs.getBoolean("LATEST_NON_REJECTED_MINOR");

        dto.lastUpdateBy = rs.getString("LAST_UPDATE_BY");
        dto.lastUpdateByLc = rs.getString("LAST_UPDATE_BY_LC");

        dto.lastStateChangeBy = rs.getString("LAST_STATE_CHANGE_BY");
        dto.lastStateChangeByLc = rs.getString("LAST_STATE_CHANGE_BY_LC");

        dto.primaryCurriculumCode = rs.getString("PRI_CODE");
        dto.primaryCurriculumCodeLc = rs.getString("PRI_CODE_LC");

        dto.primaryCurriculumDetails = rs.getString("PRI_DETAILS");
        dto.primaryCurriculumDetailsLc = rs.getString("PRI_DETAILS_LC");

        dto.proposedDatetime = getLocalDateTime(rs, "PROPOSED_DATETIME");
        dto.approvedDatetime = getLocalDateTime(rs, "APPROVED_DATETIME");
        dto.datetimeLastStateChange = getLocalDateTime(rs, "DATETIME_LAST_STATE_CHANGE");
        dto.datetimeLastUpdate = getLocalDateTime(rs, "DATETIME_LAST_UPDATE");

        dto.userCommentDescription = rs.getString("USER_COMMENT_DESCRIPTION");
        dto.userCommentDescriptionLc = rs.getString("USER_COMMENT_DESCRIPTION_LC");

        return dto;
    }

    private static LocalDateTime getLocalDateTime(ResultSet rs, String columnLabel) throws SQLException {
        Timestamp ts = rs.getTimestamp(columnLabel);
        return (ts != null) ? ts.toLocalDateTime() : null;
    }
    // Getters and setters omitted for brevity (can be generated via Lombok or IDE)
}