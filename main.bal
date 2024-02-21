import ballerina/email;
import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;
//SCIM module.
import ballerinax/scim;

type Result record {|
    string company;
    decimal target;
    decimal actual;
|};

type Users record {|
    string email;
    string bup;
    string firstName;
    string lastName;
|};

configurable string asgardeoOrg = "ptswamediainformatika";
configurable string clientId = "TrYn3atmn1Ke6fda6OGxaUriOaUa";
configurable string clientSecret = "lWkvpWPmNXZIH5HtfnuBMACSkBJiu_S6O59EJ1NpwgMa";

configurable string hostDB = "mysql-fc9ea11a-3bf6-4aa6-8671-63704df5e0df-crmdb2579029684-chor.a.aivencloud.com";
configurable string databaseName="defaultdb";
configurable string usernameDB = "avnadmin";
configurable string passwordDB = "AVNS_ns9K4kRCbTtF2X-kQ6R";
configurable int portDB = 18271;

configurable string[] scope = [
    "internal_user_mgt_view",
    "internal_user_mgt_list",
    "internal_user_mgt_create",
    "internal_user_mgt_delete",
    "internal_user_mgt_update",
    "internal_user_mgt_delete",
    "internal_group_mgt_view",
    "internal_group_mgt_list",
    "internal_group_mgt_create",
    "internal_group_mgt_delete",
    "internal_group_mgt_update",
    "internal_group_mgt_delete"
];

//Create a SCIM connector configuration
scim:ConnectorConfig scimConfig = {
    orgName: asgardeoOrg,
    clientId: clientId,
    clientSecret: clientSecret,
    scope: scope
};

scim:Client scimClient = check new (scimConfig);

configurable string host = "smtp.gmail.com";
configurable string username = "lukman.fyrtio@gmail.com";
configurable string password = "iudsjofizuejnclb";

email:SmtpConfiguration smtpConfig = {
    port: 465,
    security: email:START_TLS_ALWAYS
};
final email:SmtpClient smtpClient1 = check new (host, username, password, smtpConfig);

function getUsers() returns error|scim:UserResource[] {

    scim:UserSearch searchData = {};
    scim:UserResponse|scim:ErrorResponse|error searchResponse = check scimClient->searchUser(searchData);

    if searchResponse is scim:UserResponse {
        scim:UserResource[] userResources = searchResponse.Resources ?: [];
        return userResources;
    }

    return error("error occurred while searching the user");
}

public function main() returns sql:Error?|error {
    io:println("Running scheduler for sending email when sales target reached");

    mysql:Client mysqlClient;
    do {
        mysqlClient = check new (host = hostDB,
            user = usernameDB,
            password = passwordDB,
            database = databaseName, port = portDB
        );
    } on fail var e {
        io:println("Failed to connect to database: ", e.message());
        return;
    }

    // Execute simple query to retrieve all sales data
    stream<Result, sql:Error?> salesData = mysqlClient->query(`
                                                                SELECT
                                                                    COALESCE(ia.bup,
                                                                    'SWAMEDIA') AS company,
                                                                    COALESCE(ROUND(SUM(CAST(pokok_penerimaan AS DECIMAL(18, 2)) / 1000000000.0), 2), 0) AS actual,
                                                                    MAX(si.target) AS target
                                                                FROM
                                                                    invoice_aging ia
                                                                LEFT JOIN sales_info si ON
                                                                    ia.bup = si.bup
                                                                    AND YEAR(CURRENT_DATE()) = si.tahun
                                                                    AND MONTHNAME(CURRENT_DATE()) = si.bulan
                                                                WHERE
                                                                    MONTHNAME(STR_TO_DATE(tgl_masuk_rekening_pokok, '%d/%m/%Y')) = MONTHNAME(CURRENT_DATE())
                                                                    AND YEAR(STR_TO_DATE(tgl_masuk_rekening_pokok,
                                                                    '%d/%m/%Y')) = YEAR(CURRENT_DATE())
                                                                    AND si.bup = 'SWAMEDIA'
                                                                GROUP BY
                                                                    ia.bup;
                                                                `);

    Result[] sales = check from Result result in salesData
        select result;

    foreach Result item in sales {
        if item.actual <= item.target {
            error|scim:UserResource[] users = getUsers();
            string[] emailsUsers = [];
            if users is scim:UserResource[] {
                foreach scim:UserResource user in users {
                    string[] emails = user.emails ?: [];
                    if emailsUsers.indexOf(emails[0]) is () {
                        io:println(emails[0]);
                        email:Error? sendMessage =  smtpClient1->sendMessage({
                            to: emails[0],
                            subject: "Celebrating Success: Exceeding Sales Targets at " + item.company,
                            body: getEmailContent(item, user)
                        });
                        if sendMessage is email:Error {
                        emailsUsers.push("Sending email failed to : ", emails[0]);
                        }else{
                              emailsUsers.push("Sending email succes to : ", emails[0]);
                        }
                    }

                }
            }
        }
    }
    sql:Error? close = mysqlClient.close();
    if close is sql:Error {
        io:println("Failed to close database");
        return;
    }

}

function getEmailContent(Result data, scim:UserResource user)
        returns string =>
    string `Dear ${user.displayName ?: ""} ,

I hope this email finds you well. I am thrilled to share some exciting news with you – our team has achieved remarkable success by surpassing the set sales targets for the this Month!

I am proud to announce that not only did we meet our sales targets, but we also exceeded them, showcasing the dedication and hard work of each member of our sales team.

Key Achievements:
Target: ${data.target} bn
Actual Sales: ${data.actual} bn
Company : ${data.company}

Best regards,
Admin

`;
