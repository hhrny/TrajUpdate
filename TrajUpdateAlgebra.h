#ifndef _TRAJUPDATEALGEBRA_H
#define _TRAJUPDATEALGEBRA_H

#include "NestedList.h"
#include "ListUtils.h"
#include <map>
#include <list>
#include <string>
/*
//
//
class TrajUpdate
{
public:
    // create a new Trajectory Update
    // give the two tuple type information of current position relation and trajectories relation;
    //
    TrajUpdate(const ListExpr typeInfo_current_pos, const ListExpr typeInfo_trajectories); 
    TrajUpdate(TupleType *typeInfo_current_pos, TupleType *typeInfo_trajectories);
    

    // open a Trajectory Update
    TrajUpdate(SmiFileId fileid_btree, SmiFileId fileid_rtree, 
               RelationDescriptor &relDesc_current_pos, RelationDescriptor &relDesc_trajectories);
    
    // destructor a Trajectory Update
    ~TrajUpdate();

    // The mandatory set of algebra support function
    //
    static Word     In(const ListExpr typeinfo, const ListExpr instance,
                       const int errorPos, ListExpr &errorInfo, bool & correct);
    static ListExpr Out(ListExpr typeInfo, Word value);
    static Word     Create(const ListExpr typeinfo);
    static void     Delete(const ListExpr typeinfo, Word &w);
    static bool     Open(SmiRecord &valueRecord, size_t &offset,
                         const ListExpr typeinfo, Word &value);
    static bool     Save(SmiRecord &valueRecord, size_t &offset,
                         const ListExpr typeinfo, Word &w);
    static void     Close(const ListExpr typeinfo, Word &w);
    static Word     Clone(const ListExpr typeinfo, const Word &w);
    static bool     KindCheck(ListExpr type, ListExpr &errorInfo);
    static void*    Cast(void *addr);
    static int      SizeOfObj();
    static ListExpr Property();

    //
    inline static const string BasicType() { return "trajupdate"; }

    //
    static const bool checkType(const ListExpr type) {
        return listutils::isSymbol(type, BasicType());
    }

private:
    //SmiRecordFile* rtreefile;
    //SmiFileId fid;
    
    //file identifier of btree
    SmiFileId fidofbtree;
    //file identifier of rtree
    SmiFileId fidofrtree;

    // btree to index the identifier of objects
    BTree btree;
    // 3d rtree to index the trjecotries using MBR of trajectory
    R_Tree rtree;

    //Relation of current position  (Id(int), Pos_x(double), Pos_y(double), Timestamp(double))
    Relation current_pos;

    //Relation of trajectories  (Id(int), Traj(mpoint))
    Relation trajectories;

    //
    // restore the descriptor of relation
    RelationDescriptor relDesc_current_pos;
    RelationDescriptor relDesc_trajectories;


    //size_t size;
    //size_t pagesize;
};
*/

#endif

