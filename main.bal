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
    string departmentId;
    string companyId;
    string departmentName;
|};

type UserMapping record {|
    string email;
    string userId;
|};

type Users record {|
    string email;
    string bup;
    string firstName;
    string lastName;
|};

type EmailLog record {|
    int emailCount;
|};

configurable string asgardeoOrg = ?;
configurable string clientId = ?;
configurable string clientSecret = ?;

configurable string hostDB = ?;
configurable string databaseName = ?;
configurable string usernameDB = ?;
configurable string passwordDB = ?;
configurable int portDB = ?;
configurable string host = ?;
configurable string username = ?;
configurable string password = ?;

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

function findEmailById(string id) returns error|scim:UserResource {

    scim:UserSearch searchData = {filter: string `id eq ${id}`};
    scim:UserResponse|scim:ErrorResponse|error searchResponse = check scimClient->searchUser(searchData);

    if searchResponse is scim:UserResponse {
        scim:UserResource[] userResources = searchResponse.Resources ?: [];

        return userResources[0];
    }

    return error("error occurred while searching the user");
}

mysql:Client mysqlClient = check new (host = hostDB,
    user = usernameDB,
    password = passwordDB,
    database = databaseName, port = portDB
);

public function main() returns sql:Error?|error {
    io:println("Running scheduler for sending email when sales target reached");

    // Execute simple query to retrieve all sales data
    stream<Result, sql:Error?> salesData = mysqlClient->query(`
                                                            select
                                                                BIN_TO_UUID(sr.department_id) as departmentId,
                                                                BIN_TO_UUID(c.id) as companyId,
                                                                c.name as company,
                                                                d.name as departmentName,
                                                                COALESCE(ROUND(SUM(CAST(sr.principal_receipt AS DECIMAL(18, 2)) / 1000000000.0), 2), 0) AS actual,
                                                                MAX(ct.target) AS target
                                                            from
                                                                sales_revenue sr
                                                            left join sales_leads sl on
                                                                sl.id = sr.sales_leads_id
                                                            LEFT JOIN department d on
                                                                d.id = sr.department_id
                                                            left join company c on
                                                                c.id = d.company_id
                                                            left join company_target ct 
                                                            on
                                                                ct.department_id = d.id
                                                                AND YEAR(CURRENT_DATE()) = ct.year
                                                                AND MONTHNAME(CURRENT_DATE()) = ct.month
                                                            GROUP BY
                                                                sr.department_id
                                                            HAVING
                                                                actual >= target;
                                                                `);

    Result[] sales = check from Result result in salesData
        select result;

    foreach Result item in sales {

        stream<UserMapping, sql:Error?> usersData = mysqlClient->query(`
                                                        select
                                                            email,
                                                            user_id as userId
                                                        from
                                                            user_mapping um
                                                        where
                                                            um.company_id = UUID_TO_BIN(${item.companyId})
                                                        group by
                                                            um.email ,
                                                            user_id
                                                                `);

        UserMapping[] users = check from UserMapping result in usersData
            select result;

        foreach UserMapping user in users {
            io:println(user.toJson());
            error|scim:UserResource searchResponse = findEmailById(user.userId);
            if searchResponse is scim:UserResource {

                string displayName = string `${searchResponse?.name?.givenName ?: ""} ${searchResponse?.name?.familyName ?: ""}`;
                io:println(displayName, item.company, user, user.email);
                string subject = "Celebrating Success: Exceeding Sales Targets at " + item.company;
                string body = getEmailContent(item, displayName);
                boolean exist = check checkEmailExists(body, user.email, subject);
                if !exist {
                email:Error? sendMessage = smtpClient1->sendMessage({
                    to: user.email,
                    subject: subject,
                    body: body
                });
                if sendMessage is email:Error {
                    io:println("Sending email failed to : ", user.email,body,subject);
                } else {
                     _ = check mysqlClient->execute(`
                        INSERT INTO defaultdb.email_notification_log
                        (time_received, sender_email, subject, body)
                        VALUES(  CURRENT_TIMESTAMP, ${user.email}, ${subject}, ${body});`);
                    io:println("Sending email succes to : ", user.email);
                }
                }else{
                      io:println("Email sudah dikirim sebelumnya ke "+user.email);
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

function getEmailContent(Result data, string name)

    returns string =>
    string `Dear ${name},

I hope this email finds you well. I am thrilled to share some exciting news with you – our team has achieved remarkable success by surpassing the set sales targets for this month!

I am proud to announce that not only did we meet our sales targets, but we also exceeded them, showcasing the dedication and hard work of each member of our sales team.

Key Achievements:
- Target: ${data.target} billion
- Actual Sales: ${data.actual} billion
- Department Name: ${data.departmentName}
- Company: ${data.company}

This outstanding accomplishment reflects the commitment and teamwork that define our company culture. It's a testament to your efforts and contribution to our collective success.

Thank you for your hard work and dedication. Let's continue to set new heights and achieve even greater milestones together.

Best regards,
Admin
`;

function checkEmailExists(string body, string email, string subject) returns boolean|error {

    stream<EmailLog, sql:Error?> emailLogs = mysqlClient->query(`SELECT COUNT(*) AS emailCount FROM email_notification_log
                             WHERE email = ${email} AND body = ${body} AND subject = ${subject}`);

    // Process the stream and convert results to Album[] or return error.
    EmailLog[] emailLogs1 = check from EmailLog emailLog in emailLogs
        select emailLog;
        io:println(emailLogs1[0].emailCount);
    return emailLogs1[0].emailCount > 0;
}

