#include<bits/stdc++.h>
using namespace std;

vector<vector<int>>aa,ca;
vector<int>ind;
int n;

int bond(int a, int b){
    if(a==0){
        return 0;
    }
    if(b==n+1){
        return 0;
    }
    int sum = 0;
    for(int i=0;i<n;i++){
        sum+=aa[i][a-1]*aa[i][b-1];
    }
    return sum;
}

int main(){
    ofstream fout("output.txt");
    
    aa = {
        {-1,0,45,0},
        {0,-1,5,75},
        {45,5,-1,3},
        {0,75,3,-1}
    };

    set<int>st;
    n = aa.size();
    ind = {1,2};  // already given order
    ind.push_back(n+1);
    ind.insert(ind.begin(),0);

    for(int i=1;i<=n;i++)st.insert(i);
    for(auto p:ind)st.erase(p);

    for(int i=0;i<n;i++){
        int sum = 0;
        for(int j=0;j<n;j++){
            if(aa[j][i]!=-1)sum+=aa[j][i];
        }
        aa[i][i] = sum;
    }

    // for(int i=0;i<n;i++){
    //     for(int j=0;j<n;j++){
    //         cout<<aa[i][j]<<" ";
    //     }
    //     cout<<endl;
    // }

    for(auto cur:st){
        vector<pair<int,int>>vp;
        for(int i=0;i<ind.size()-1;i++){
            int res = 2*bond(ind[i],cur)+2*bond(cur,ind[i+1])-2*bond(ind[i],ind[i+1]);
            vp.push_back({res,i});
        }

        sort(vp.begin(),vp.end());
        reverse(vp.begin(),vp.end());
        ind.insert(ind.begin()+vp[0].second+1,cur);
    }

    ind.erase(ind.begin());
    ind.pop_back();

    // fout<<"The order of the elements is: "<<endl;
    fout<<"   ";
    for(auto p:ind)fout<<p<<" ";
    fout<<endl;
    for(auto p:ind)fout<<"---";
    fout<<endl;

    for(int i=0;i<ind.size();i++){
        fout<<ind[i]<<"| ";
        for(int j=0;j<ind.size();j++){
            fout<<aa[ind[i]-1][ind[j]-1]<<" ";
        }
        fout<<endl;
    }
}
