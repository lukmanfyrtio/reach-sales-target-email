import ballerina/email;
import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;

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

configurable string host = "smtp.gmail.com";
configurable string username = "lukman.fyrtio@gmail.com";
configurable string password = "iudsjofizuejnclb";

email:SmtpConfiguration smtpConfig = {
    port: 465,
    security: email:START_TLS_ALWAYS
};
final email:SmtpClient smtpClient1 = check new (host, username, password, smtpConfig);

public function main() returns error? {
    mysql:Client mysqlClient;
    do {
        mysqlClient = check new (host = "mysql-fc9ea11a-3bf6-4aa6-8671-63704df5e0df-crmdb2579029684-chor.a.aivencloud.com",
            user = "avnadmin",
            password = "AVNS_ns9K4kRCbTtF2X-kQ6R",
            database = "defaultdb", port = 18271
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

    // Process the stream and convert results to Artist[] or return error.
    Result item2;
    check from Result item in salesData
        do {
            item2 = item;
            //if sales target reached
            if item.actual >= item.target {

                stream<Users, sql:Error?> usersStream = mysqlClient->query(`select u.email,u.bup,u.first_name as firstName,u.last_name as lastName from users u where u.bup=${item.company}`);

                check from Users user in usersStream
                    do {
                        io:println(user.email);
                        lock {
                            check smtpClient1->sendMessage({
                                            to: user.email,
                                            subject: "Celebrating Success: Exceeding Sales Targets at " + item2.company,
                                            body: getEmailContent(item2, user)
                                    });
                        }
                    };

            }

        };
    _ = check mysqlClient.close();
    return;
}

function getEmailContent(Result data, Users user)
        returns string =>
    string `Dear ${user.firstName} ${user.lastName},

I hope this email finds you well. I am thrilled to share some exciting news with you – our team has achieved remarkable success by surpassing the set sales targets for the this Month!

I am proud to announce that not only did we meet our sales targets, but we also exceeded them, showcasing the dedication and hard work of each member of our sales team.

Key Achievements:
Target: ${data.target} bn
Actual Sales: ${data.actual} bn
Company : ${user.bup}

Best regards,
Admin

`;
