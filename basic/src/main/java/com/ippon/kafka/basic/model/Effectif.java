package com.ippon.kafka.basic.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Created by Flo on 04/03/2017.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Effectif {

    public Effectif() {
    }

    public Effectif(int year, String schoolYear, String geographicLevel,
                    String geographicUnit, String group, String establishmenGroup,
                    String sector, String establishmentSector, String sex,
                    String sexDesc, Double studentCount, String dutStudents,
                    Double dutStudentsCount, String ingStudents, Double ingStudentsCount,
                    String freeData, String data, String secret, String secretData,
                    String geoLevel, String geoLevelId) {
        this.year = year;
        this.schoolYear = schoolYear;
        this.geographicLevel = geographicLevel;
        this.geographicUnit = geographicUnit;
        this.group = group;
        this.establishmenGroup = establishmenGroup;
        this.sector = sector;
        this.establishmentSector = establishmentSector;
        this.sex = sex;
        this.sexDesc = sexDesc;
        this.studentCount = studentCount;
        this.dutStudents = dutStudents;
        this.dutStudentsCount = dutStudentsCount;
        this.ingStudents = ingStudents;
        this.ingStudentsCount = ingStudentsCount;
        this.freeData = freeData;
        this.data = data;
        this.secret = secret;
        this.secretData = secretData;
        this.geoLevel = geoLevel;
        this.geoLevelId = geoLevelId;
    }

    /**
     * rentrée
     */
    private int year;

    // Rentrée universitaire
    private String schoolYear;

    // Niveau géographique
    private String geographicLevel;

    // Unité géographique
    private String geographicUnit;

    // regroupement
    private String group;

    // Regroupements de formations ou d’établissements
    private String establishmenGroup;

    // secteur
    private String sector;

    // Secteur de l’établissement d’inscription
    private String establishmentSector;

    // sexe
    private String sex;

    // Sexe de l’étudiant
    private String sexDesc;

    // Nombre total d’étudiants inscrits
    private Double studentCount;

    // A des effectifs en formations de DUT
    private String dutStudents;

    // Nombre d’étudiants inscrits en DUT
    private Double dutStudentsCount;

    // A des effectifs en formations d'ingénieurs
    private String ingStudents;

    // Nombre d’étudiants inscrits dans les formations d’Ingénieur
    private Double ingStudentsCount;

    // diffusable
    private String freeData;

    // Données diffusables
    private String data;

    // secret
    private String secret;

    // Données soumises au secret statistique
    private String secretData;

    // niveau_geo
    private String geoLevel;

    // Identifiant de l’unité géographique
    private String geoLevelId;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getSchoolYear() {
        return schoolYear;
    }

    public void setSchoolYear(String schoolYear) {
        this.schoolYear = schoolYear;
    }

    public String getGeographicLevel() {
        return geographicLevel;
    }

    public void setGeographicLevel(String geographicLevel) {
        this.geographicLevel = geographicLevel;
    }

    public String getGeographicUnit() {
        return geographicUnit;
    }

    public void setGeographicUnit(String geographicUnit) {
        this.geographicUnit = geographicUnit;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getEstablishmenGroup() {
        return establishmenGroup;
    }

    public void setEstablishmenGroup(String establishmenGroup) {
        this.establishmenGroup = establishmenGroup;
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public String getEstablishmentSector() {
        return establishmentSector;
    }

    public void setEstablishmentSector(String establishmentSector) {
        this.establishmentSector = establishmentSector;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getSexDesc() {
        return sexDesc;
    }

    public void setSexDesc(String sexDesc) {
        this.sexDesc = sexDesc;
    }

    public Double getStudentCount() {
        return studentCount;
    }

    public void setStudentCount(Double studentCount) {
        this.studentCount = studentCount;
    }

    public String getDutStudents() {
        return dutStudents;
    }

    public void setDutStudents(String dutStudents) {
        this.dutStudents = dutStudents;
    }

    public Double getDutStudentsCount() {
        return dutStudentsCount;
    }

    public void setDutStudentsCount(Double dutStudentsCount) {
        this.dutStudentsCount = dutStudentsCount;
    }

    public String getIngStudents() {
        return ingStudents;
    }

    public void setIngStudents(String ingStudents) {
        this.ingStudents = ingStudents;
    }

    public Double getIngStudentsCount() {
        return ingStudentsCount;
    }

    public void setIngStudentsCount(Double ingStudentsCount) {
        this.ingStudentsCount = ingStudentsCount;
    }

    public String getFreeData() {
        return freeData;
    }

    public void setFreeData(String freeData) {
        this.freeData = freeData;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getSecretData() {
        return secretData;
    }

    public void setSecretData(String secretData) {
        this.secretData = secretData;
    }

    public String getGeoLevel() {
        return geoLevel;
    }

    public void setGeoLevel(String geoLevel) {
        this.geoLevel = geoLevel;
    }

    public String getGeoLevelId() {
        return geoLevelId;
    }

    public void setGeoLevelId(String geoLevelId) {
        this.geoLevelId = geoLevelId;
    }
}
