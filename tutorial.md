# Tutorial For Course Participants

当前内容处于实验阶段。

## 如何参与课程

本课程使用 [Github Classroom](https://classroom.github.com/) 进行作业内容管理。如果你想参与本课程，可以通过我们创建的 [作业模版](https://classroom.github.com/a/aioto_CO) 接收作业内容，它将会自动为你生成一个作业仓库，该仓库位于 xxx 组织下。

## 本地工作

你可以通过如下命令在本地获取到你的作业：

```
git clone https://github.com/{Org name}/tinysql-template-{Your name}.git
```

该作业模版基于 [TinySQL](https://github.com/pingcap-incubator/tinysql) 创建，你可以通过如下命令获取最新的内容更新。

```
git remote add upstream https://github.com/pingcap-incubator/tinysql
git remote set-url --push upstream no_push
git fetch upstream
git checkout master
git rebase upstream/master
```

## 提交作业

我们采用 CI 工具对你的作业内容进行评估，每一次的 push 到 master 仓库，都会触发 CI 过程。我们将测试你提交的代码的正确性，并且计算相应的得分情况。得分结果将会通过邮件的形式告知，邮件目的地为参与者的 Github 注册邮箱。

可以通过如下命令提交作业：

```
git add .
git commit -m"{Your Commit Message}"
git push origin master
```
