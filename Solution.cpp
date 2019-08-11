#include <fstream>
#include <vector>
#include<unordered_map>
#include<unordered_set>
#include<map>
#include <algorithm>
#include <iostream>
#include<cmath>
#include <functional>

using namespace std;

enum class CaseResult
{
    CASE_PASS = 1,
    CASE_FAIL
};

struct CaseRecord
{
    unsigned int id{ 0 };
    unsigned int testCaseId{ 0 };
    unsigned int buildId{ 0 };
    unsigned int teamId{ 0 };
    unsigned int result{ 0 };
    unsigned int phaseId{ 0 };
};

class TeamRecord
{
public:
    void aggregate(const CaseRecord& p_record)
    {
        m_teamId = p_record.teamId;
        bool l_isCasePass = (CaseResult::CASE_PASS == static_cast<CaseResult>(p_record.result));
        const auto it = testCaseWithResult.emplace(p_record.testCaseId, l_isCasePass);
        if (it.second)
        {
            p_record.result == 1 ? ++m_passNum : ++m_failNum;
        }
        else if(!testCaseWithResult[p_record.testCaseId] && l_isCasePass)
        {
            ++m_passNum;
            --m_failNum;
        }
    }
    void print(ostream& p_out) const
    {
        unsigned int l_caseNum = testCaseWithResult.size();
        p_out << "          {\n";
        p_out << "            \"case_num\": " << l_caseNum << ",\n";
        p_out << "            \"team_id\": \"" << m_teamId << "\",\n";
        p_out << "            \"execution_passed\": " << m_passNum << ",\n";
        p_out << "            \"execution_failed\": " << m_failNum << ",\n";
        p_out << "            \"pass_rate\": \"" << round(m_passNum * 100.0 / l_caseNum) << "%\"\n";
        p_out << "          }";
    }

private:
    unsigned int m_teamId{ 0 };
    unsigned int m_passNum{ 0 };
    unsigned int m_failNum{ 0 };
    unordered_map<unsigned int, bool> testCaseWithResult;
};

class PhaseRecord
{
public:
    void aggregate(const CaseRecord& p_record)
    {
        if (-1 == m_recordId)
        {
            m_recordId = p_record.id;
        }
        m_phaseId = p_record.phaseId;
        auto& l_teamRecord = teamRecords[p_record.teamId];
        l_teamRecord.aggregate(p_record);
        testCaseIdWithTeamIds.insert((p_record.teamId << 31) + p_record.testCaseId);
    }

    void print(ostream& p_out) const
    {
        p_out << "      {\n";
        p_out << "        \"phase_id\": \"" << m_phaseId << "\"" << ",\n";
        p_out << "        \"team_num\": " << teamRecords.size() << ",\n";
        p_out << "        \"case_num\": " << testCaseIdWithTeamIds.size() << ",\n";
        p_out << "        \"test_info\": [\n";
        for (auto it = teamRecords.begin(); it != teamRecords.end(); ++it)
        {
            if (it != teamRecords.begin())
            {
                p_out << ",\n";
            }
            it->second.print(p_out);
        }
        p_out << endl << "        ]\n";
        p_out << "      }";
    }

private:
    unsigned int m_phaseId{ 0 };
    int m_recordId{ -1 };
    map<unsigned int, TeamRecord> teamRecords;
    unordered_set<long> testCaseIdWithTeamIds;
};

class BuildRecord
{
public:
    void aggregate(const CaseRecord& p_record)
    {
        m_buildId = p_record.buildId;

        auto it = phaseRecords.emplace(p_record.phaseId, PhaseRecord());
        if (it.second)
        {
            phaseRecordQueue.push_back(ref(it.first->second));
        }

        it.first->second.aggregate(p_record);
    }
    void print(ostream& p_out) const
    {
        p_out << "  {\n";
        p_out << "    \"phase\": [\n";
        for (auto it = phaseRecordQueue.begin(); it != phaseRecordQueue.end(); ++it)
        {
            if (it != phaseRecordQueue.begin())
            {
                p_out << ",\n";
            }
            it->get().print(p_out);
        }
        p_out << endl << "    ],\n";
        p_out << "    \"build_id\": \"" << m_buildId << "\"\n";
        p_out << "  }";

    }

private:
    unsigned int m_buildId{ 0 };
    map<unsigned int, PhaseRecord> phaseRecords;
    vector<reference_wrapper<PhaseRecord>> phaseRecordQueue;
};

using CaseRecords = vector<CaseRecord>;

class CsvReader
{
public:
    explicit CsvReader(const char* p_file) : m_file(p_file)
    {
    }
    CsvReader(const CsvReader&) = delete;
    CsvReader(CsvReader&&) = delete;
    CsvReader& operator =(const CsvReader&) = delete;
    CsvReader& operator =(CsvReader&&) = delete;
    ~CsvReader() = default;

    CaseRecords parse()
    {
        ifstream l_file;
        l_file.open(m_file, ios::in);

        if (!l_file.is_open())
        {
            return {};
        }

        char l_commas{ 0 };
        char l_executionTime[64]{ 0 };
        CaseRecords l_caseRecords;

        string l_header;
        l_file >> l_header;
        while (!l_file.eof())
        {
            CaseRecord l_record;
            l_file >> l_record.id >> l_commas
                >> l_record.testCaseId >> l_commas
                >> l_record.buildId >> l_commas
                >> l_record.teamId >> l_commas;
            l_file.getline(l_executionTime, sizeof(l_executionTime) - 1, ',');
            l_file >> l_record.result >> l_commas
                >> l_record.phaseId >> l_commas;

            l_caseRecords.push_back(move(l_record));
        }
        l_file.close();
        return l_caseRecords;
    }

private:
    string m_file;
};

class RecordStat
{
public:
    RecordStat() {}
    void aggregate(const CaseRecords& p_records)
    {
        for_each(p_records.begin(), p_records.end(), [this](const CaseRecord & p_record) {
            auto& l_buildRecord = m_buildRecords[p_record.buildId];
            l_buildRecord.aggregate(p_record);
            }
        );
    }
    const map<unsigned int, BuildRecord>& getRecords()
    {
        return m_buildRecords;
    }
private:
    map<unsigned int, BuildRecord> m_buildRecords;
};

class Printer
{
public:
    explicit Printer(ostream &stream = cout) 
        : m_out(stream)
    {}
    Printer(const Printer&) = delete;
    Printer(Printer&&) = delete;
    Printer& operator =(const Printer&) = delete;
    Printer& operator =(Printer&&) = delete;
    ~Printer() = default;

    void print(const map<unsigned int, BuildRecord>& p_records)
    {
        m_out << "[\n";
        for (auto it = p_records.begin(); it != p_records.end(); ++it)
        {
            if (it != p_records.begin())
            {
                m_out << ",\n";
            }
            it->second.print(m_out);
        }
        m_out << endl << "]\n";
    }
private:
    ostream& m_out;
};

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        return -1;
    }
    CsvReader l_reader(argv[1]);
    RecordStat recordStat;
    recordStat.aggregate(l_reader.parse());
    Printer printer;
    printer.print(recordStat.getRecords());
    return 0;
}
