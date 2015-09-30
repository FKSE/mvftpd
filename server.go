package main

import (
    "github.com/op/go-logging"
    "github.com/yob/graval"
    "gopkg.in/mgo.v2"
    "os"
    "time"
    "io"
    "gopkg.in/mgo.v2/bson"
    "path"
)

//init log
var log = logging.MustGetLogger("mvftpd")

type User struct {
    Id bson.ObjectId `bson:"_id,omitempty" json:"_id"`
    Username string `bson:"username" json:"username"`
    Password string `bson:"password json:"password"`
    Salt string `bson:"salt" json:"salt"`
}

type FileMeta struct {
    Owner bson.ObjectId `bson:"owner,omitempty" json:"owner"`
}

// Define MongoDB Driver
type MongoDriver struct {
    Database *mgo.Database
    User *User
}

func findFileByName (name string, driver *MongoDriver) (file *mgo.GridFile)  {
    gfs := driver.Database.GridFS("files")
    iter := gfs.Find(bson.M{"filename": name, "metadata.owner": driver.User.Id}).Iter()
    gfs.OpenNext(iter, &file)
    return file
}

func (driver *MongoDriver) Authenticate(user string, pass string) bool {
    //find user
    c := driver.Database.C("users")
    // init user
    driver.User = new(User)
    err := c.Find(bson.M{"username": user, "password": pass}).One(driver.User)
    log.Info("%v", driver.User)
    return err == nil
}
func (driver *MongoDriver) Bytes(path string) (bytes int) {
    return 0
}
func (driver *MongoDriver) ModifiedTime(path string) (time.Time, error) {
    return time.Now(), nil
}
// We don't have any directories
func (driver *MongoDriver) ChangeDir(path string) bool {
    return false
}
func (driver *MongoDriver) DirContents(path string) (files []os.FileInfo) {
    files = []os.FileInfo{}

    gfs := driver.Database.GridFS("files")

    iter := gfs.Find(bson.M{"metadata.owner": driver.User.Id}).Iter()
    //result
    var file *mgo.GridFile
    //loop over result set
    for gfs.OpenNext(iter, &file) {
        files = append(files, graval.NewFileItem(file.Name(), int(file.Size())))
    }

    return files
}
// We don't have any directories
func (driver *MongoDriver) DeleteDir(name string) bool {
    name = path.Base(name)
    // check for existing files
    file := findFileByName(name, driver)
    // delete existing file
    if file != nil {
        err := driver.Database.GridFS("files").RemoveId(file.Id())
        if err != nil {
            log.Error(err.Error())
            return false
        }
    }

    return false
}
func (driver *MongoDriver) DeleteFile(path string) bool {
    return false
}
func (driver *MongoDriver) Rename(fromPath string, toPath string) bool {
    return false
}
func (driver *MongoDriver) MakeDir(path string) bool {
    return false
}
func (driver *MongoDriver) GetFile(path string) (data string, err error) {
    return "", nil
}
func (driver *MongoDriver) PutFile(destPath string, data io.Reader) bool {
    name := path.Base(destPath)
    // check for existing files
    oldFile := findFileByName(name, driver)
    // delete existing file
    if oldFile != nil {
        driver.Database.GridFS("files").RemoveId(oldFile.Id())
    }
    // create new file
    file, err := driver.Database.GridFS("files").Create(name)
    // Set file owner
    file.SetMeta(FileMeta{Owner: driver.User.Id})
    if err != nil {
        log.Fatal(err.Error())
        return false
    }
    // copy data to target
    _, err = io.Copy(file, data)
    if err != nil {
        log.Fatal(err.Error())
        return false
    }
    file.Close();

    return true
}

type MongoDriverFactory struct{
    Database *mgo.Database
}

func (factory *MongoDriverFactory) NewDriver() (graval.FTPDriver, error) {
    // init connection with database session
    return &MongoDriver{Database: factory.Database}, nil
}



//cool logging format
var format = logging.MustStringFormatter(
    "%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

func main() {
    //for now we log to stdout
    backend := logging.NewLogBackend(os.Stdout, "", 0)
    //put backend to formatter
    backend2Formatter := logging.NewBackendFormatter(backend, format)
    logging.SetBackend(backend, backend2Formatter)
    //load config
    log.Info("Server starting")
    // Connect to MongoDB
    mongoSession, err := mgo.Dial("localhost")
    if err != nil {
        panic(err)
    }
    defer mongoSession.Close()
    //get collection
    database := mongoSession.DB("ftpserver")
    // Create factory
    factory := &MongoDriverFactory{Database:database}
    ftpServer := graval.NewFTPServer(&graval.FTPServerOpts{ Factory: factory })
    err = ftpServer.ListenAndServe()
    if err != nil {
        log.Error(err.Error())
        log.Fatal("Error starting server!")
    }

}
