#include "generator.h"
#include "util.h"

std::mutex ZipfWrapper::gen_mtx;
std::map<std::string, WorkloadFile *> ZipfWrapper::wf_map;

std::mutex dataset_mtx;

WorkloadFile::WorkloadFile(std::string filename) {
    std::ifstream fin;
    fin.open(filename, std::ios::in | std::ios::binary);
    fin.seekg(0, std::ios::end);
    int size = fin.tellg();

    bufsize = size / sizeof(int);
    buffer = new int[bufsize];

    fin.seekg(0);
    fin.read((char *)buffer, sizeof(int) * bufsize);
    fin.close();
}

static const uint64_t kFNVPrime64 = 1099511628211;
unsigned int hashfunc(uint32_t val) {
    uint32_t hash = 123;
    int i;
    for (i = 0; i < sizeof(uint32_t); i++) {
        uint64_t octet = val & 0x00ff;
        val = val >> 8;

        hash = hash ^ octet;
        hash = hash * kFNVPrime64;
    }
    return hash;
}

int ZipfGenerator::randomInt() {
    double d = rdm.randomDouble();

    int low = 0, high = size;
    while (low < high - 1) {
        int mid = (low + high) / 2;
        if (zipfs[mid] <= d && zipfs[mid + 1] > d) {
            low = mid;
            break;
        } else if (zipfs[mid] > d) {
            high = mid;
        } else {
            low = mid;
        }
    }
    return hashfunc(low) % size;
}

void ZipfGenerator::init(double s, int inital) {
    zipfs = new double[inital];
    double sum = 0.0;
    for (int i = 1; i < inital + 1; i++) {
        zipfs[i - 1] = 1.0 / (float)pow((double)i, s);
        sum += zipfs[i - 1];
    }
    zipfs[0] = 1.0 / sum;
    for (int i = 1; i < inital; i++) {
        zipfs[i] = zipfs[i] / sum + zipfs[i - 1];
    }
}

ZipfGenerator::ZipfGenerator(double s, int inital) : size(inital) {
    init(s, inital);
}

ZipfWrapper::ZipfWrapper(double s, int inital) {
    cursor = random();
    std::string filename = get_file_name(s);
    gen_mtx.lock();
    if (wf_map.find(filename) == wf_map.end()) {
        if (access(filename.c_str(), 0)) {
            std::cout << filename << " not exists, generate it now\n";
            ZipfGenerator zipf(s, inital);
            std::ofstream myfile;
            myfile.open(filename, std::ios::out | std::ios::binary);
            for (unsigned long long i = 0; i < inital * 16; i++) {
                int d = zipf.randomInt();
                myfile.write((char *)&d, sizeof(int));
            }
            myfile.close();
        }

        wf_map[filename] = new WorkloadFile(filename);
    }
    wf = wf_map[filename];
    gen_mtx.unlock();
}

DataSet::DataSet(int size, int key_length, int email)
    : data_size(size), key_len(key_length), emailkey(email) {
    if (emailkey == 0) { // rand string key
        std::string fn_str = get_file_name_str(key_len);
        dataset_mtx.lock();
        if (access(fn_str.c_str(), 0)) {
            std::cout << fn_str << " not exist, generate it now\n";
            RandomGenerator rdm(key_len);
            std::ofstream myfile;
            myfile.open(fn_str, std::ios::out);
            for (unsigned long long i = 0; i < data_size; i++) {
                std::string s = rdm.RandomStr();
                myfile << s << "\n";
            }
            myfile.close();
        }

        std::cout << "start to load data\n";
        std::ifstream fstr;
        wl_str = new std::string[data_size];
        fstr.open(fn_str, std::ios::in);
        for (int i = 0; i < data_size; i++) {
            fstr >> wl_str[i];
            assert(wl_str[i].length()>0);
        }
        fstr.close();
        dataset_mtx.unlock();
        std::cout << "load random string key successfully\n";
    } else { // email key
        std::string email_key_file = "/tmp/email_key";
        dataset_mtx.lock();
        std::ifstream fstr;
        wl_str = new std::string[data_size];
        fstr.open(email_key_file, std::ios::in);
        for (int i = 0; i < data_size; i++) {
            fstr >> wl_str[i];
        }
        fstr.close();
        dataset_mtx.unlock();
        std::cout << "load string data successfully\n";
    }
}
