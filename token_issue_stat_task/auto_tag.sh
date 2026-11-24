#!/bin/bash
tag=$(git describe --tags `git rev-list --tags --max-count=1`)
branch=(`git branch | grep \* | cut -d ' ' -f2`)
echo "更新前版本" $tag
echo "更新的分支" $branch
echo "更新备注" $2
noVTag=${tag:1}
array=(${noVTag//./ })
arrayLen=${#array[*]}
if [[ $arrayLen != 3 ]];then
  echo "版本号长度不对，必须为 v1.x.x的格式"
  exit
elif [[ $1 -lt 1 ]]||[[ $1 -gt 3 ]]; then
  echo "参数必须在1-3之间"
  exit
elif [[ $2 == "" ]]; then
   echo "tag备注必填"
  exit
fi

version=""
for (( i = 0; i < $arrayLen; i++ )); do
    v=${array[i]}
    if [[ $1 == 1 ]];then
      if [[ $i == 0 ]];then
        v=$(echo $v+1 | bc)
      elif [[ $i == 1 ]]||[[ $i == 2 ]];then
        v=0
      fi
    elif [[ $1 == 2 ]];then
      if [[ $i == 1 ]];then
        v=$(echo $v+1 | bc)
      elif [[ $i == 2 ]];then
        v=0
      fi
    elif [[ $1 == 3 ]];then
      if [[ $i == 2 ]];then
        v=$(echo $v+1 | bc)
      fi
    fi
    version+=$v"."
done
newVersion=v${version%?}
echo "新版本："$newVersion
git tag -a $newVersion -m ${branch}_${version%?}_$2
git push origin $newVersion