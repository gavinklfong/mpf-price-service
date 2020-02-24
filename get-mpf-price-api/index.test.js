const { trusteeList, trustee } = require('./index');
const mpfDataAccess = require('./mpf-data-access');

jest.mock('./mpf-data-access');

const mockTrusteeList = ["HSBC", "ABC"];
const eventTrusteeList = require('./event.json');

describe("Retrieve Trustee List", () => {

    it("should test", () => {
        expect(trusteeList).toBeDefined();
    });

    it("should be a function", () => {
        expect(typeof trusteeList).toBe("function");
    });

    it("should get trusteeList", () => {
        mpfDataAccess.getTrusteeList.mockResolvedValue(mockTrusteeList);
        trusteeList(eventTrusteeList).then(item => {
            expect(item.statusCode).toEqual(200);
            expect(JSON.parse(item.body)).toContain(mockTrusteeList);
        })

    });

    it("should get trusteeList with exception", () => {
        mpfDataAccess.getTrusteeList.mockImplementation(() => {
            throw error("mock error");
        });
        trusteeList(eventTrusteeList).then(item => {
            expect(item.statusCode).toEqual(500);
        })

    });
});


describe("Retrieve Trustee", () => {
    it("should test", () => {
        expect(trustee).toBeDefined();
    });

    it("should be a function", () => {
        expect(typeof trustee).toBe("function");
    });


});