using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void Test1()
    {
      Assert.AreEqual("World",
                      HelloWorld.Hello);
    }
}
