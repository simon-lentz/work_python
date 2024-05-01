# neomodel_inspect_database --db bolt://neo4j_username:neo4j_password@localhost:7687 -T yourapp/models.py
from neomodel import StructuredNode, StringProperty, RelationshipTo, OneOrMore, FloatProperty


class State(StructuredNode):
    uid = StringProperty()
    stateName = StringProperty()
    stateFIPS = StringProperty()


class County(StructuredNode):
    uid = StringProperty()
    countyName = StringProperty()
    countyFIPS = StringProperty()
    stateName = StringProperty()
    stateFIPS = StringProperty()
    located_in_state = RelationshipTo("State", "LOCATED_IN_State", cardinality=OneOrMore)


class City(StructuredNode):
    uid = StringProperty()
    countyName = StringProperty()
    countyFIPS = StringProperty()
    cbsaTitle = StringProperty()
    cbsaCode = StringProperty()
    stateName = StringProperty()
    stateFIPS = StringProperty()
    located_in_county = RelationshipTo("County", "LOCATED_IN_County", cardinality=OneOrMore)


class Issuer(StructuredNode):
    uid = StringProperty()
    issuerID = StringProperty()


class StateIssuer(StructuredNode):
    uid = StringProperty()
    dateRetrieved = StringProperty()
    stateAbbreviation = StringProperty()
    msrbIssuerIdentifier = StringProperty()
    issuerName = StringProperty()
    stateFIPS = StringProperty()
    issuerHomepage = StringProperty()
    msrbIssuerType = StringProperty()
    issues_in_state = RelationshipTo("State", "ISSUES_IN_State", cardinality=OneOrMore)


class OtherIssuer(StructuredNode):
    pass


class CountyIssuer(StructuredNode):
    uid = StringProperty()
    dateRetrieved = StringProperty()
    countyName = StringProperty()
    countyFIPS = StringProperty()
    stateAbbreviation = StringProperty()
    msrbIssuerIdentifier = StringProperty()
    issuerName = StringProperty()
    stateFIPS = StringProperty()
    issuerHomepage = StringProperty()
    msrbIssuerType = StringProperty()
    type_of_issuer = RelationshipTo("Issuer", "TYPE_OF_Issuer", cardinality=OneOrMore)


class CityIssuer(StructuredNode):
    uid = StringProperty()
    dateRetrieved = StringProperty()
    countyName = StringProperty()
    countyFIPS = StringProperty()
    cbsaTitle = StringProperty()
    cbsaCode = StringProperty()
    stateAbbreviation = StringProperty()
    msrbIssuerIdentifier = StringProperty()
    issuerName = StringProperty()
    stateFIPS = StringProperty()
    issuerHomepage = StringProperty()
    msrbIssuerType = StringProperty()
    type_of_issuer = RelationshipTo("Issuer", "TYPE_OF_Issuer", cardinality=OneOrMore)


class Issue(StructuredNode):
    uid = StringProperty()
    dateRetrieved = StringProperty()
    msrbIssueIdentifier = StringProperty()
    stateAbbreviation = StringProperty()
    issueHomepage = StringProperty()
    msrbIssuerIdentifier = StringProperty()
    issueDescription = StringProperty()
    datedDate = StringProperty()
    issuerName = StringProperty()
    maturityDate = StringProperty()
    stateFIPS = StringProperty()
    officialStatement = StringProperty()
    issued_by_issuer = RelationshipTo("Issuer", "ISSUED_BY_Issuer", cardinality=OneOrMore)


class Bond(StructuredNode):
    uid = StringProperty()
    msrbIssueIdentifier = StringProperty()
    offerYieldPct = FloatProperty()
    kbraRating = StringProperty()
    issueDescription = StringProperty()
    maturityDate = StringProperty()
    fitchRating = StringProperty()
    securityHomepage = StringProperty()
    dateRetrieved = StringProperty()
    offerPricePct = FloatProperty()
    principal = FloatProperty()
    msrbSecurityIdentifier = StringProperty()
    securityDescription = StringProperty()
    spRating = StringProperty()
    cusip = StringProperty()
    coupon = FloatProperty()
    moodysRating = StringProperty()
    component_of_issue = RelationshipTo("Issue", "COMPONENT_OF_Issue", cardinality=OneOrMore)
